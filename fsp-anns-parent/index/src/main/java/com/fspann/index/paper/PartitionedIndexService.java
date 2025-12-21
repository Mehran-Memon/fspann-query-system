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
    private static final int DEFAULT_BUILD_THRESHOLD =
            Math.max(20_000, Runtime.getRuntime().availableProcessors() * 20_000);

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
            ThreadLocal.withInitial(() -> new HashSet<>(2048));
    private final ThreadLocal<Integer> probeOverride =
            ThreadLocal.withInitial(() -> -1);

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

//    @Override
//    public List<EncryptedPoint> lookup(QueryToken token) {
//        throw new UnsupportedOperationException(
//                "lookup() is disabled. Use lookupCandidateIds() instead."
//        );
//    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "token cannot be null");
        Set<String> deletedCache = new HashSet<>();

        if (!frozen) {
            throw new IllegalStateException("Index not finalized before lookup");
        }

        // ===== SAFETY: lookup() is PAPER ONLY =====
        if (cfg.getSearchMode() != com.fspann.config.SearchMode.PAPER_BASELINE) {
            throw new IllegalStateException(
                    "lookup() is restricted to PAPER_BASELINE. Use lookupCandidateIds() for real runs."
            );
        }

        final SystemConfig.PaperConfig pc = cfg.getPaper();
        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * token.getTopK();
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();

        final int HARD_CAP;
        if (paperCap > 0) {
            HARD_CAP = Math.min(runtimeCap, paperCap);
        } else {
            HARD_CAP = runtimeCap;
        }

        Set<String> touchedIds = lastTouchedIds.get();
        touchedIds.clear();

        int dim = token.getDimension();
        DimensionState S = dims.get(dim);
        if (S == null) return Collections.emptyList();

        BitSet[] qcodes = token.getCodes();
        if (qcodes == null || qcodes.length == 0) {
            lastTouched.set(0);
            lastTouchedIds.get().clear();
            return List.of();
        }
        LinkedHashMap<String, EncryptedPoint> out = new LinkedHashMap<>(HARD_CAP);

        final int perDivBits = perDivisionBits();

        // ===== PAPER-FAITHFUL PREFIX SCAN (BOUNDED) =====
        int maxRelax = cfg.getRuntime().getMaxRelaxationDepth();
        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax); relax++){
            int earlyStop = cfg.getRuntime().getEarlyStopCandidates();
            if (earlyStop > 0 && out.size() >= earlyStop) {
                break;
            }

            int relaxedBits = perDivBits - (relax * pc.m);
            if (relaxedBits <= 0) continue;

            int safeDivs = Math.min(S.divisions.size(), qcodes.length);

            for (int d = 0; d < safeDivs; d++) {
                DivisionState div = S.divisions.get(d);
                BitSet qc = qcodes[d];

                for (GreedyPartitioner.SubsetBounds sb : div.I) {

                    boolean match = (relax == 0)
                            ? covers(sb, qc, relaxedBits)
                            : coversRelaxed(sb, qc, relaxedBits);

                    if (!match) continue;

                    List<String> ids = div.tagToIds.get(sb.tag);
                    if (ids == null) continue;

                    for (String id : ids) {
                        touchedIds.add(id);

                        if (out.containsKey(id)) continue;
                        if (deletedCache.contains(id)) continue;
                        if (metadata.isDeleted(id)) {
                            deletedCache.add(id);
                            continue;
                        }

                        try {
                            EncryptedPoint ep = metadata.loadEncryptedPoint(id);
                            if (ep != null) {
                                out.put(id, ep);
                            }
                        } catch (Exception ignore) {}

                        // ===== HARD STOP =====
                        if (out.size() >= HARD_CAP) {
                            lastTouched.set(touchedIds.size());
                            return new ArrayList<>(out.values());
                        }
                    }
                }
            }
        }

        lastTouched.set(touchedIds.size());
        return new ArrayList<>(out.values());
    }

    public List<String> lookupCandidateIds(QueryToken token) {
        Objects.requireNonNull(token, "token");
        int touched = 0;

        if (!frozen) {
            throw new IllegalStateException("Index not finalized");
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int K = token.getTopK();

        int runtimeCap = cfg.getRuntime().getMaxCandidateFactor() * K;
        int paperCap = cfg.getPaper().getSafetyMaxCandidates();

        final int MAX_IDS;
        if (paperCap > 0) {
            MAX_IDS = Math.min(runtimeCap, paperCap);
        } else {
            MAX_IDS = runtimeCap;
        }

        Set<String> seen = new LinkedHashSet<>(MAX_IDS);
        Set<String> deletedCache = new HashSet<>();

        DimensionState S = dims.get(token.getDimension());
        if (S == null) return List.of();

        BitSet[] qcodes = token.getCodes();

        final int perDivBits = perDivisionBits();
        int maxRelax = cfg.getRuntime().getMaxRelaxationDepth();

        for (int relax = 0; relax <= Math.min(pc.lambda, maxRelax); relax++) {
            int earlyStop = cfg.getRuntime().getEarlyStopCandidates();
            if (earlyStop > 0 && seen.size() >= earlyStop) break;

            int bits = perDivBits - relax * pc.m;
            if (bits <= 0) break;

            int safeDivs = Math.min(S.divisions.size(), qcodes.length);

            for (int d = 0; d < safeDivs; d++) {
                DivisionState div = S.divisions.get(d);
                BitSet qc = qcodes[d];

                for (GreedyPartitioner.SubsetBounds sb : div.I) {
                    if (!covers(sb, qc, bits)) continue;

                    List<String> ids = div.tagToIds.get(sb.tag);
                    if (ids == null) continue;

                    for (String id : ids) {
                        touched++;
                        if (deletedCache.contains(id)) continue;
                        if (metadata.isDeleted(id)) {
                            deletedCache.add(id);
                            continue;
                        }

                        seen.add(id);
                        if (seen.size() >= MAX_IDS) {
                            lastTouched.set(touched);
                            lastTouchedIds.get().clear();
                            lastTouchedIds.get().addAll(seen);
                            return new ArrayList<>(seen);
                        }
                    }
                }
            }
        }

        lastTouched.set(touched);
        lastTouchedIds.get().clear();
        lastTouchedIds.get().addAll(seen);

        return new ArrayList<>(seen);
    }

    public EncryptedPoint loadPointIfActive(String id) {
        if (metadata.isDeleted(id)) return null;
        try {
            return metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean covers(GreedyPartitioner.SubsetBounds sb, BitSet c, int bits) {
        var cmp = new GreedyPartitioner.CodeComparator(bits);
        return cmp.compare(sb.lower, c) <= 0 &&
                cmp.compare(c, sb.upper) <= 0;
    }

    private int perDivisionBits() {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.m * pc.lambda;
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

    private int totalCodeBits() {
        SystemConfig.PaperConfig pc = cfg.getPaper();
        return pc.divisions * pc.m * pc.lambda;
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
    public void setProbeOverride(int probes) {
        probeOverride.set(probes);
    }
    public void clearProbeOverride() {
        probeOverride.remove();
    }
    private boolean coversRelaxed(GreedyPartitioner.SubsetBounds sb, BitSet c, int bits) {
        var cmp = new GreedyPartitioner.CodeComparator(bits);
        return cmp.compare(sb.lower, c) <= 0 &&
                cmp.compare(c, sb.upper) <= 0;
    }
}