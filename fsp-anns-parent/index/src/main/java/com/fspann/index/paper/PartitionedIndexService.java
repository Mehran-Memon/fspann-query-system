package com.fspann.index.paper;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.crypto.AesGcmCryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PartitionedIndexService (mSANNP)
 *
 * - ID-only partitions
 * - No LSH
 * - No intersection
 * - Stable arrival order
 * - Compatible with D1 limiter
 * - Full CRUD support (Insert, Read, Update, Delete)
  */
public final class PartitionedIndexService implements IndexService {

    private static final Logger logger =
            LoggerFactory.getLogger(PartitionedIndexService.class);

    // =====================================================
    // CONFIGURATION CONSTANTS
    // =====================================================

    /** Build trigger threshold: create partitions when staged >= this */
    private static final int DEFAULT_BUILD_THRESHOLD = 10000;

    // =====================================================
    // FIELDS
    // =====================================================
    private final RocksDBMetadataManager metadata;
    private final SystemConfig cfg;
    private final StorageMetrics storageMetrics;
    private final KeyRotationServiceImpl keyService;
    private final AesGcmCryptoService cryptoService;

    // dim -> state
    private final Map<Integer, DimensionState> dims = new ConcurrentHashMap<>();

    // Finalization state
    private volatile boolean frozen = false;

    // ===== Query metrics =====
    private final ThreadLocal<Integer> lastTouched =
            ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Set<String>> lastTouchedIds =
            ThreadLocal.withInitial(HashSet::new);
    int touched = 0;

    // =====================================================
    // INNER CLASSES
    // =====================================================
    private static final class DimensionState {
        final int dim;
        final List<DivisionState> divisions = new ArrayList<>();
        final List<EncryptedPoint> staged = new ArrayList<>();
        final List<BitSet[]> stagedCodes = new ArrayList<>();

        DimensionState(int dim) {
            this.dim = dim;
        }
    }

    private static final class DivisionState {
        List<GreedyPartitioner.SubsetBounds> I = List.of();
        Map<String, List<String>> tagToIds = new HashMap<>();
    }

    // =====================================================
    // CONSTRUCTOR
    // =====================================================
    public PartitionedIndexService(
            RocksDBMetadataManager metadata,
            SystemConfig cfg,
            KeyRotationServiceImpl keyService,
            AesGcmCryptoService cryptoService) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.cryptoService = Objects.requireNonNull(cryptoService, "cryptoService");

        this.storageMetrics = metadata.getStorageMetrics();
        if (this.storageMetrics == null) {
            throw new IllegalStateException(
                    "StorageMetrics not available from RocksDBMetadataManager"
            );
        }

        logger.info("PartitionedIndexService initialized with CryptoService and default build threshold: {}",
                DEFAULT_BUILD_THRESHOLD);
    }

    // =====================================================
    // INSERT
    // =====================================================
    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(vector, "vector cannot be null");

        int dim = vector.length;

        // This properly encrypts the vector and returns all required fields
        EncryptedPoint ep;
        try {
            // CryptoService.encrypt() returns a properly constructed EncryptedPoint with:
            // - id, shardId, iv, ciphertext, version, vectorLength, buckets
            ep = cryptoService.encrypt(id, vector, keyService.getCurrentVersion());

            if (ep == null) {
                logger.error("CryptoService returned null EncryptedPoint for id={}", id);
                throw new RuntimeException("Failed to encrypt vector");
            }
        } catch (Exception e) {
            logger.error("Failed to encrypt vector {}: {}", id, e.getMessage());
            throw new RuntimeException("Encryption failed for vector " + id, e);
        }

        insert(ep, vector);
    }

    public void insert(EncryptedPoint pt, double[] vec) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        Objects.requireNonNull(vec, "vector cannot be null");

        int dim = vec.length;
        DimensionState S = dims.computeIfAbsent(dim, DimensionState::new);

        BitSet[] codes = code(vec);

        synchronized (S) {
            // ===== CRITICAL FIX: Persist IMMEDIATELY before staging =====
            try {
                metadata.saveEncryptedPoint(pt);
                if (logger.isDebugEnabled()) {
                    logger.debug("Persisted encrypted point {} to metadata", pt.getId());
                }
            } catch (IOException e) {
                logger.error("Failed to persist encrypted point {}: {}", pt.getId(), e.getMessage());
                throw new RuntimeException("Persistence failed for point " + pt.getId(), e);
            }

            S.staged.add(pt);
            S.stagedCodes.add(codes);

            // (SystemConfig.Paper doesn't have buildThreshold property)
            if (S.staged.size() >= DEFAULT_BUILD_THRESHOLD) {
                build(S);

                // Update storage metrics after build
                try {
                    storageMetrics.updateDimensionStorage(dim);
                } catch (Exception e) {
                    logger.warn("Failed to update storage metrics after insert", e);
                }
            }
        }
    }

    // =====================================================
    // BUILD - Partitioning (Algorithm-2)
    // =====================================================

    private void build(DimensionState S) {
        var pc = cfg.getPaper();
        int divisions = pc.divisions;
        int m = pc.m;

        S.divisions.clear();

        for (int d = 0; d < divisions; d++) {
            List<GreedyPartitioner.Item> items = new ArrayList<>();

            for (int i = 0; i < S.staged.size(); i++) {
                items.add(new GreedyPartitioner.Item(
                        S.staged.get(i).getId(),
                        S.stagedCodes.get(i)[d]
                ));
            }
            int codeBits = pc.m * pc.lambda;

            var br = GreedyPartitioner.build(
                    items,
                    codeBits,
                    pc.seed + d
            );

            DivisionState div = new DivisionState();
            div.I = br.indexI;
            div.tagToIds = br.tagToIds;

            S.divisions.add(div);
        }

        S.staged.clear();
        S.stagedCodes.clear();

        logger.debug("Built partitions for dimension {}: {} divisions", S.dim, S.divisions.size());
    }

    // =====================================================
    // LOOKUP (Algorithm-3 prefix order)
    // =====================================================

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "token cannot be null");
        lastTouched.set(0);
        int touched =0;
        Set<String> touchedIds = lastTouchedIds.get();
        touchedIds.clear();

        if (!frozen) {
            throw new IllegalStateException("Index not finalized before lookup");
        }

        int dim = token.getDimension();
        DimensionState S = dims.get(dim);
        if (S == null) return Collections.emptyList();

        BitSet[] qcodes = token.getCodes();
        int lambda = token.getLambda();

        LinkedHashMap<String, EncryptedPoint> out = new LinkedHashMap<>();

        for (int d = 0; d < S.divisions.size(); d++) {
            DivisionState div = S.divisions.get(d);
            BitSet qc = qcodes[d];

            int probesUsed = 0;
            int probeLimit = cfg.getPaper().probeLimit;

            for (GreedyPartitioner.SubsetBounds sb : div.I) {

                if (!covers(sb, qc)) continue;

                if (probesUsed >= probeLimit) break;
                probesUsed++;

                List<String> ids = div.tagToIds.get(sb.tag);
                if (ids == null) continue;

                for (String id : ids) {
                    touched++;
                    touchedIds.add(id);
                    if (out.containsKey(id)) continue;
                    if (metadata.isDeleted(id)) continue;

                    try {
                        EncryptedPoint ep = metadata.loadEncryptedPoint(id);
                        if (ep == null) continue;
                        KeyVersion kvp = keyService.getVersion(ep.getKeyVersion());
                        if (kvp == null) {
                            continue; // truly unrecoverable
                        }

                        out.put(id, ep);
                    } catch (Exception e) {
                        logger.warn("Failed to load encrypted point {}", id, e);
                    }
                }
            }

            logger.debug(
                    "lookup: dim={} division={} probeLimit={} probesUsed={}",
                    dim, d, probeLimit, probesUsed
            );
        }

        for (EncryptedPoint ep : S.staged) {
            String id = ep.getId();
            lastTouched.set(lastTouched.get() + 1);
            if (out.containsKey(id)) continue;
            if (metadata.isDeleted(id)) continue;

            out.put(id, ep);
        }
        lastTouched.set(touched);
        return new ArrayList<>(out.values());
    }

    private boolean covers(GreedyPartitioner.SubsetBounds sb, BitSet c) {
        var cmp = new GreedyPartitioner.CodeComparator(sb.codeBits);
        return cmp.compare(sb.lower, c) <= 0 &&
                cmp.compare(c, sb.upper) <= 0;
    }

    // =====================================================
    // DELETE
    // =====================================================
    public void delete(String id) {
        Objects.requireNonNull(id, "id cannot be null");

        logger.debug("Deleting vector: {}", id);

        // STEP 1: Load the encrypted point to verify it exists
        EncryptedPoint ep;
        try {
            ep = metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            logger.warn("Cannot load point {} for deletion: {}", id, e.getMessage());
            return;
        }

        if (ep == null) {
            logger.warn("Point {} not found for deletion", id);
            return;
        }

        int vecDim = ep.getVectorLength();
        logger.debug("Loaded point {} (dimension={})", id, vecDim);

        // STEP 2: Mark as deleted in metadata
        try {
            Map<String, String> meta = metadata.getVectorMetadata(id);
            meta.put("deleted", "true");
            meta.put("deleted_at", String.valueOf(System.currentTimeMillis()));
            meta.put("deleted_by_op", "direct_delete");
            metadata.updateVectorMetadata(id, meta);
            logger.debug("Marked {} as deleted in metadata", id);
        } catch (Exception e) {
            logger.error("Failed to mark {} as deleted in metadata", id, e);
            return;
        }

        // STEP 3: Remove from partition structure (only if frozen)
        if (frozen) {
            try {
                DimensionState dimState = dims.get(vecDim);
                if (dimState != null) {
                    synchronized (dimState) {
                        for (DivisionState divState : dimState.divisions) {
                            for (List<String> ids : divState.tagToIds.values()) {
                                ids.remove(id);
                            }
                        }
                    }
                }
                logger.debug("Removed {} from partition structure for dim={}", id, vecDim);
            } catch (Exception e) {
                logger.warn("Failed to remove {} from partition structure", id, e);
            }
        } else {
            logger.debug("Index not frozen yet, skipping partition cleanup for {}", id);
        }

        // STEP 4: Update storage metrics
        try {
            storageMetrics.updateDimensionStorage(vecDim);
            logger.debug("Updated storage metrics for dimension {}", vecDim);
        } catch (Exception e) {
            logger.warn("Failed to update storage metrics after delete of {}", id, e);
        }

        logger.info("Successfully deleted vector {}", id);
    }

    /**
     * Delete multiple vectors in batch.
     */
    public void deleteAll(List<String> ids) {
        Objects.requireNonNull(ids, "ids list cannot be null");
        logger.info("Batch deleting {} vectors", ids.size());

        for (String id : ids) {
            try {
                delete(id);
            } catch (Exception e) {
                logger.warn("Failed to delete vector {}", id, e);
            }
        }

        logger.info("Batch delete completed");
    }

    // =====================================================
    // UPDATE
    // =====================================================
    /**
     * Update a vector (logically: mark old as deleted + insert new).
     *
     * Constraints:
     *  - Vector must exist
     *  - Dimension must match
     *  - Index must NOT be frozen (updates only during buffering phase)
     *
     * @param id the vector ID
     * @param newVector the new vector data
     * @throws IllegalArgumentException if vector not found or dimension mismatch
     * @throws IllegalStateException if index is frozen
     */
    public void update(String id, double[] newVector) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(newVector, "newVector cannot be null");

        logger.debug("Updating vector: {}, new dimension: {}", id, newVector.length);

        // STEP 1: Load old point to verify existence and dimension
        EncryptedPoint oldPoint;
        try {
            oldPoint = metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot load point for update: " + id, e
            );
        }

        if (oldPoint == null) {
            throw new IllegalArgumentException(
                    "Point not found for update: " + id
            );
        }

        int oldDim = oldPoint.getVectorLength();
        int newDim = newVector.length;

        logger.debug("Loaded old point {} (old dimension: {})", id, oldDim);

        // STEP 2: Verify dimension match
        if (oldDim != newDim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Dimension mismatch for update of %s: old=%d, new=%d. " +
                                    "Cannot change dimension of existing vector.",
                            id, oldDim, newDim
                    )
            );
        }

        // STEP 3: Check index is not frozen
        if (frozen) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot update finalized index for dimension %d. " +
                                    "Index is frozen. Create a new dimension or request re-opening for updates.",
                            oldDim
                    )
            );
        }

        // STEP 4: Delete old (mark as deleted)
        try {
            logger.debug("Marking old vector {} as deleted", id);
            Map<String, String> oldMeta = metadata.getVectorMetadata(id);
            oldMeta.put("deleted", "true");
            oldMeta.put("deleted_at", String.valueOf(System.currentTimeMillis()));
            oldMeta.put("updated_from", id);
            metadata.updateVectorMetadata(id, oldMeta);
        } catch (Exception e) {
            logger.error("Failed to mark old vector {} as deleted during update", id, e);
            throw new RuntimeException("Update failed: could not mark old vector as deleted", e);
        }

        // STEP 5: Insert new vector with same ID
        try {
            logger.debug("Inserting new vector {} (dimension: {})", id, newDim);
            insert(id, newVector);
        } catch (Exception e) {
            logger.error("Failed to insert new vector {} during update", id, e);
            throw new RuntimeException("Update failed: could not insert new vector", e);
        }

        // STEP 6: Update metadata with version and timestamp
        try {
            Map<String, String> newMeta = metadata.getVectorMetadata(id);
            newMeta.put("updated_at", String.valueOf(System.currentTimeMillis()));
            if (keyService != null) {
                newMeta.put("updated_version", String.valueOf(keyService.getCurrentVersion().getVersion()));
            }
            newMeta.put("update_type", "full_replacement");
            metadata.updateVectorMetadata(id, newMeta);
            logger.debug("Updated metadata for vector {}", id);
        } catch (Exception e) {
            logger.warn("Failed to update metadata version for {}", id, e);
        }

        // STEP 7: Update storage metrics
        try {
            storageMetrics.updateDimensionStorage(newDim);
        } catch (Exception e) {
            logger.warn("Failed to update storage metrics during update of {}", id, e);
        }

        logger.info(
                "Successfully updated vector {} (dimension: {}, old version retained as deleted)",
                id, newDim
        );
    }

    /**
     * Update multiple vectors in batch.
     */
    public void updateBatch(Map<String, double[]> updates) {
        Objects.requireNonNull(updates, "updates map cannot be null");

        logger.info("Batch updating {} vectors", updates.size());

        int successCount = 0;
        int failureCount = 0;

        for (Map.Entry<String, double[]> entry : updates.entrySet()) {
            String id = entry.getKey();
            double[] newVector = entry.getValue();

            try {
                update(id, newVector);
                successCount++;
            } catch (Exception e) {
                logger.warn("Failed to update vector {} in batch", id, e);
                failureCount++;
            }
        }

        logger.info("Batch update complete: {} success, {} failed", successCount, failureCount);
    }

    // =====================================================
    // FINALIZATION
    // =====================================================
    /**
     * Finalize index for search (freeze partitions).
     * After this, no inserts/updates allowed, queries enabled.
     */
    public void finalizeForSearch() {
        if (frozen) {
            logger.info("Index already finalized");
            return;
        }

        logger.info("Finalizing index for search...");

        // Build any remaining staged vectors
        for (DimensionState S : dims.values()) {
            synchronized (S) {
                if (!S.staged.isEmpty()) {
                    logger.debug("Building staged vectors for dim={} before finalization", S.dim);
                    build(S);
                }
            }
        }

        // Freeze state
        frozen = true;

        // Update storage metrics
        for (DimensionState S : dims.values()) {
            try {
                storageMetrics.updateDimensionStorage(S.dim);
                StorageMetrics.StorageSnapshot snap = storageMetrics.getSnapshot();
                logger.info("Finalized dimension {}: {}", S.dim, snap.summary());
            } catch (Exception e) {
                logger.warn("Failed to update storage metrics for dimension {}", S.dim, e);
            }
        }

        logger.info("Index finalization complete");
    }

    /**
     * Check if index is frozen (finalized).
     */
    public boolean isFrozen() {
        return frozen;
    }

    /**
     * Check if specific dimension is finalized.
     */
    public boolean isDimensionFinalized(int dim) {
        DimensionState S = dims.get(dim);
        return S != null && !S.divisions.isEmpty();
    }

    /**
     * Check if all registered dimensions are finalized.
     */
    public boolean areAllDimensionsFinalized() {
        if (dims.isEmpty()) return false;

        for (DimensionState S : dims.values()) {
            if (S.divisions.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // =====================================================
    // STORAGE METRICS
    // =====================================================
    /**
     * Get storage metrics instance.
     */
    public StorageMetrics getStorageMetrics() {
        return storageMetrics;
    }

    /**
     * Get current storage snapshot.
     */
    public StorageMetrics.StorageSnapshot getStorageSnapshot() {
        return storageMetrics.getSnapshot();
    }

    /**
     * Count deleted vectors in the partition structure.
     */
    public int countDeletedInPartition() {
        int deletedCount = 0;

        for (DimensionState S : dims.values()) {
            synchronized (S) {
                for (DivisionState div : S.divisions) {
                    for (List<String> ids : div.tagToIds.values()) {
                        for (String id : ids) {
                            if (metadata.isDeleted(id)) {
                                deletedCount++;
                            }
                        }
                    }
                }
            }
        }

        return deletedCount;
    }

    /**
     * Get count of active (non-deleted) vectors in partition.
     */
    public int countActiveInPartition() {
        Set<String> seen = new HashSet<>();

        for (DimensionState S : dims.values()) {
            synchronized (S) {
                for (DivisionState div : S.divisions) {
                    for (List<String> ids : div.tagToIds.values()) {
                        for (String id : ids) {
                            if (!metadata.isDeleted(id)) {
                                seen.add(id);
                            }
                        }
                    }
                }
            }
        }
        return seen.size();
    }

    // =====================================================
    // CODING (Algorithm-1)
    // =====================================================
    /**
     * Generate bit-interleaved codes for all divisions.
     */
    public BitSet[] code(double[] vec) {
        SystemConfig.PaperConfig pc = cfg.getPaper();

        int m = pc.m;
        int lambda = pc.lambda;
        int divisions = pc.divisions;
        long seed = pc.seed;

        logger.debug(
                "Coding: dim={} m={} lambda={} divisions={} seed={}",
                vec.length,
                m,
                lambda,
                divisions,
                seed
        );

        return Coding.code(
                vec,
                divisions,
                m,
                lambda,
                seed
        );

    }


    // =====================================================
    // REQUIRED IndexService METHODS
    // =====================================================

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return null; // No buffer in this implementation
    }
    public Set<String> getLastTouchedIds() {
        return Collections.unmodifiableSet(lastTouchedIds.get());
    }
    public int getLastTouchedCount() {
        return lastTouched.get();
    }
}