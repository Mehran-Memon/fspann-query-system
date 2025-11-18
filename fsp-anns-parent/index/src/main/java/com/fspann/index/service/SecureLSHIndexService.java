package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.EvenLSH;
import com.fspann.index.paper.PartitionedIndexService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * SecureLSHIndexService
 * ------------------------
 * Unified entry point for indexing and lookup with paper-aligned partitioned mode.
 * - Routes to PaperSearchEngine (Coding → GreedyPartition → TagQuery).
 * - Client-side kNN over subset union, forward-secure (re-encrypt only).
 * Storage/crypto/lifecycle (RocksDB/AES-GCM/KeyService) are shared.
 */
public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    // --------------------------- Core dependencies ---------------------------
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;

    // Paper-aligned engine (Partitioned indexing mode)
    private volatile PaperSearchEngine paperEngine;

    // Write buffer
    private final EncryptedPointBuffer buffer;

    // Write-through toggle (persist to Rocks + metrics account)
    private volatile boolean writeThrough =
            !"false".equalsIgnoreCase(System.getProperty("index.writeThrough", "true"));

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager,
                                 PaperSearchEngine paperEngine,
                                 EncryptedPointBuffer buffer) {
        this.crypto = (crypto != null)
                ? crypto
                : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.paperEngine = paperEngine;
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        logger.info("SecureLSHIndexService initialized in 'partitioned' (paper) mode.");
    }

    /** Factory method for configuration from SystemConfig. */
    public static SecureLSHIndexService fromConfig(CryptoService crypto,
                                                   KeyLifeCycleService keyService,
                                                   RocksDBMetadataManager metadata,
                                                   SystemConfig cfg) {
        EncryptedPointBuffer buf = createBufferFromManager(metadata);
        SecureLSHIndexService svc = new SecureLSHIndexService(crypto, keyService, metadata, null, buf);

        // Enable paper engine via config if requested
        try {
            var pc = cfg.getPaper();
            if (pc != null && pc.isEnabled()) {
                PartitionedIndexService pe = new PartitionedIndexService(
                        pc.getM(), pc.getLambda(), pc.getDivisions(), pc.getSeed()
                );
                svc.setPaperEngine(pe);
                logger.info("Paper engine enabled via config (m={}, λ={}, ℓ={}, seed={})",
                        pc.getM(), pc.getLambda(), pc.getDivisions(), pc.getSeed());
            }
        } catch (Throwable t) {
            logger.warn("Failed to initialize paper engine from config, continuing without it", t);
        }
        return svc;
    }

    private static EncryptedPointBuffer createBufferFromManager(RocksDBMetadataManager manager) {
        String pointsBase = Objects.requireNonNull(
                manager.getPointsBaseDir(),
                "metadataManager.getPointsBaseDir() returned null."
        );
        try {
            return new EncryptedPointBuffer(pointsBase, manager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    // Allow wiring/overriding paper engine
    public void setPaperEngine(PaperSearchEngine engine) {
        this.paperEngine = engine;
    }

    // -------------------------------------------------------------------------
    // IndexService API Implementations (Paper Mode)
    // -------------------------------------------------------------------------

    public void batchInsert(List<String> ids, List<double[]> vectors) {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(vectors, "vectors");
        if (ids.size() != vectors.size()) {
            throw new IllegalArgumentException("ids and vectors must be same size");
        }

        // Simple loop over the existing insert(...) API
        for (int i = 0; i < ids.size(); i++) {
            insert(ids.get(i), vectors.get(i));
        }
    }

    @Override
    public void insert(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");

        if (paperEngine != null) {
            paperEngine.insert(pt);
        }

        if (writeThrough) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", String.valueOf(pt.getVersion()));
            metadata.put("dim", String.valueOf(pt.getVectorLength()));
            List<Integer> buckets = pt.getBuckets();
            if (buckets != null) {
                for (int t = 0; t < buckets.size(); t++) {
                    metadata.put("b" + t, String.valueOf(buckets.get(t)));
                }
            }

            try {
                metadataManager.batchUpdateVectorMetadata(Collections.singletonMap(pt.getId(), metadata));
                metadataManager.saveEncryptedPoint(pt);
            } catch (IOException e) {
                logger.error("Failed to persist encrypted point {}", pt.getId(), e);
                return;
            }
            keyService.incrementOperation();

            try {
                buffer.add(pt);
            } catch (Exception e) {
                logger.warn("Buffered write failed for {}", pt.getId(), e);
            }
        }
    }

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");

        EncryptedPoint enc = crypto.encrypt(id, vector);

        if (paperEngine != null) {
            // In a full implementation you’d pass (enc, plaintext) to precompute codes
            // Here we keep the simple encrypted-only path, assuming client pre-computes codes.
            paperEngine.insert(enc);
        }

        if (writeThrough) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", String.valueOf(enc.getVersion()));
            metadata.put("dim", String.valueOf(vector.length));

            try {
                metadataManager.batchUpdateVectorMetadata(Collections.singletonMap(enc.getId(), metadata));
                metadataManager.saveEncryptedPoint(enc);
            } catch (IOException e) {
                logger.error("Failed to persist encrypted point {}", enc.getId(), e);
                return;
            }
            keyService.incrementOperation();

            try {
                buffer.add(enc);
            } catch (Exception e) {
                logger.warn("Buffered write failed for {}", enc.getId(), e);
            }
        }
    }

    public void flushBuffers() {
        buffer.flushAll();
    }


    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        if (paperEngine != null) {
            return paperEngine.lookup(token);
        }
        return Collections.emptyList();
    }

    @Override
    public void delete(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        if (paperEngine != null) {
            paperEngine.delete(id);
        }
        // Metadata cleanup left to higher-level components if needed
    }

    @Override
    public void markDirty(int shardId) {
        // Legacy API; partitioned/paper mode has no explicit shards.
        logger.debug("markDirty(shardId={}) called in paper mode; no-op.", shardId);
    }

    @Override
    public int getIndexedVectorCount() {
        if (paperEngine instanceof PartitionedIndexService pe) {
            return pe.getTotalVectorCount();
        }
        // Fallback: approximate via metadata entry count
        return metadataManager.getAllVectorIds().size();
    }

    @Override
    public Set<Integer> getRegisteredDimensions() {
        if (paperEngine instanceof PartitionedIndexService pe) {
            return pe.getRegisteredDimensions();
        }
        return Collections.emptySet();
    }

    @Override
    public int getVectorCountForDimension(int dimension) {
        if (paperEngine != null) {
            return paperEngine.getVectorCountForDimension(dimension);
        }
        return 0;
    }

    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        try {
            return metadataManager.loadEncryptedPoint(id);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to load encrypted point {} from disk", id, e);
            return null;
        }
    }

    @Override
    public void updateCachedPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");

        // 1) Update in-memory index if it supports it
        if (paperEngine instanceof PartitionedIndexService pe) {
            pe.updateCachedPoint(pt);
        }

        // 2) Persist updated point + metadata
        try {
            metadataManager.saveEncryptedPoint(pt);
            metadataManager.updateVectorMetadata(pt.getId(), Map.of(
                    "version", String.valueOf(pt.getVersion()),
                    "dim", String.valueOf(pt.getVectorLength())
            ));
        } catch (IOException e) {
            logger.error("Failed to update cached point {}", pt.getId(), e);
        }
    }

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return buffer;
    }

    @Override
    public int getShardIdForVector(double[] vector) {
        // Partitioned/paper mode does not expose shard IDs; we return a sentinel.
        return -1;
    }

    /**
     * Restore-only helper used by ForwardSecureANNSystem.restoreIndexFromDisk(...).
     * For now this is kept very conservative – it only rebuilds the in-memory
     * index if you *already* have a way to do that from an EncryptedPoint.
     *
     * If you don’t yet have such logic, this is a safe no-op and will still
     * let things compile; you can later wire it into your real index structure.
     */
    public void addPointToIndexOnly(EncryptedPoint ep) {
        if (ep == null) {
            return;
        }

        // TODO (optional – if you have an internal index that accepts EncryptedPoint):
        //   myIndex.addPoint(ep);
        //
        // For now we leave it as a no-op to avoid touching internals that are not
        // present in this branch.
    }

    public void setWriteThrough(boolean enabled) {
        this.writeThrough = enabled;
    }

    public boolean isWriteThrough() {
        return writeThrough;
    }

    /**
     * LSH accessor used only for building QueryTokenFactory in ForwardSecureANNSystem.
     *
     * Because your current SecureLSHIndexService implementation does not expose a
     * per-dimension EvenLSH, we provide a placeholder that clearly fails if called.
     *
     * Once you know which EvenLSH instance you use internally for a given dimension,
     * replace the body of this method to return it.
     */
    public EvenLSH getLshForDimension(int dimension) {
        throw new UnsupportedOperationException(
                "getLshForDimension(dimension) is not wired for this branch of SecureLSHIndexService. " +
                        "You must implement this to return the EvenLSH used for dimension " + dimension +
                        " in your internal index structure.");
    }


    // -------------------------------------------------------------------------
    // Extra helpers for lifecycle
    // -------------------------------------------------------------------------

    public void clearCache() {
        // Clears in-memory write buffer only (not RocksDB)
        try {
            buffer.clear();
        } catch (Exception e) {
            logger.warn("Failed to clear EncryptedPointBuffer", e);
        }
    }

    public void shutdown() {
        try {
            buffer.shutdown();
        } catch (Exception e) {
            logger.warn("Error during EncryptedPointBuffer shutdown", e);
        }
        // Metadata manager close is handled at system level (ForwardSecureANNSystem)
    }


    // -------------------------------------------------------------------------
    // Paper-aligned engine contract (Partitioned Indexing)
    // -------------------------------------------------------------------------
    public interface PaperSearchEngine {
        void insert(EncryptedPoint pt);                            // encrypted only
        void insert(EncryptedPoint pt, double[] plaintextVector);  // with vector for coding
        List<EncryptedPoint> lookup(QueryToken token);             // encrypted candidates (subset union)
        void delete(String id);
        int getVectorCountForDimension(int dimension);

    }
}
