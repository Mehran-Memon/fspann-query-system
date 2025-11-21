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
import java.util.concurrent.ConcurrentHashMap;

/**
 * SecureLSHIndexService
 * ------------------------
 * Unified entry point for indexing and lookup with paper-aligned partitioned mode.
 *
 * Modes:
 *  - Legacy (paperEngine == null):
 *      * insert(id, vector) -> encrypt + persist + optional buffer
 *      * lookup(token)      -> base scan over RocksDB (all vectors)
 *
 *  - Paper mode with real PartitionedIndexService:
 *      * insert(id, vector) -> encrypt + persist + server-side coding into PartitionedIndexService
 *      * lookup(token)      -> delegates to PartitionedIndexService (uses token.codes BitSets)
 *
 *  - Paper mode with mock / non-partition engine (tests):
 *      * insert(id, vector) -> encrypt + persist, then throws UnsupportedOperationException
 *        (to signal “this engine requires precomputed codes”).
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

    // Optional: per-dimension EvenLSH registry for QueryTokenFactory / legacy tooling
    private final Map<Integer, EvenLSH> lshPerDimension = new ConcurrentHashMap<>();

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
        logger.info("SecureLSHIndexService initialized. paperEnginePresent={}", (paperEngine != null));
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
    // IndexService API Implementations (Paper Mode + Fallback)
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

        // NOTE:
        //  - For PartitionedIndexService we *do not* hand off this encrypted-only
        //    insert, because it has no plaintext to generate codes from.
        //  - Tests that care about “no codes engine” semantics exercise insert(id, vector)
        //    against a mock, not this method.
        if (paperEngine != null && !(paperEngine instanceof PartitionedIndexService)) {
            // Non-partition paper engine (probably a test double) can still get the raw point.
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

        // 1) Encrypt
        EncryptedPoint enc = crypto.encrypt(id, vector);

        // 2) Persist to RocksDB + buffer (independent of codes engine)
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

        // 3) Behaviour by mode:
        //   - No paper engine            -> legacy behaviour: just persist (base-scan handles lookup).
        //   - Mock paper engine (tests)  -> throw UnsupportedOperationException *after* persistence.
        //   - Real PartitionedIndexService -> server-side coding + partitioned insert.
        PaperSearchEngine engine = this.paperEngine;
        if (engine == null) {
            // legacy mode: just persist and return
            return;
        }

        if (!(engine instanceof PartitionedIndexService pe)) {
            // This is exactly the path that SecureLSHIndexService*PaperModeTest asserts on:
            // "Paper mode must reject plaintext insert(id, vector) when there is no codes engine."
            throw new UnsupportedOperationException(
                    "This SecureLSHIndexService requires precomputed codes in paper mode; " +
                            "plain insert(id, vector) is not supported when paperEngine is not a PartitionedIndexService."
            );
        }

        // Real partitioned paper engine: compute codes server-side and insert into partitions.
        // This uses PartitionedIndexService.insert(EncryptedPoint, double[]) which wraps code()
        // and insertWithCodes(CodedPoint).
        pe.insert(enc, vector);
    }

    public void flushBuffers() {
        buffer.flushAll();
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        // ------------------------ Paper / partitioned mode -------------------
        if (paperEngine != null) {
            return paperEngine.lookup(token);
        }

        // ------------------------ Legacy fallback: base scan -----------------
        // When no paper engine is configured (e.g., older configs, very small fixtures),
        // we approximate a "base scan" by pulling *all* encrypted points from
        // RocksDB via metadata manager. QueryServiceImpl will handle decryption
        // and ranking, and ratio tests can still compare against this baseline.
        List<String> ids = metadataManager.getAllVectorIds();
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }

        List<EncryptedPoint> result = new ArrayList<>(ids.size());
        for (String id : ids) {
            try {
                EncryptedPoint ep = metadataManager.loadEncryptedPoint(id);
                if (ep != null) {
                    result.add(ep);
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.warn("Failed to load encrypted point {} during lookup", id, e);
            }
        }

        logger.debug("Legacy lookup() returned {} candidates (paperEngine == null)", result.size());
        return result;
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
        // Fallback: we don't track per-dimension counts without paper engine.
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

    public void addPointToIndexOnly(EncryptedPoint ep) {
        if (ep == null) {
            return;
        }
        // No-op placeholder for restore-only scenarios.
    }

    public void setWriteThrough(boolean enabled) {
        this.writeThrough = enabled;
    }

    public boolean isWriteThrough() {
        return writeThrough;
    }

    /**
     * Provide an EvenLSH instance for a given dimension, primarily for
     * QueryTokenFactory / legacy tooling that still expects an LSH object.
     *
     * For the current paper-focused branch, these LSH instances are used
     * mainly to build bucket IDs for tokens; the actual candidate retrieval
     * may be handled by either:
     *   - PartitionedIndexService (paperEngine != null) using codes, or
     *   - Metadata base-scan (paperEngine == null), ignoring buckets.
     */
    public EvenLSH getLshForDimension(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be > 0");
        }
        return lshPerDimension.computeIfAbsent(dimension, dim -> {
            int m = Integer.getInteger("token.lsh.m", 8);
            int r = Integer.getInteger("token.lsh.r", 4);
            long seed = Long.getLong("token.lsh.seed", 13L);
            logger.debug("getLshForDimension: creating EvenLSH(m={}, dim={}, r={}, seed={})",
                    m, dim, r, seed);
            return new EvenLSH(m, dim, r, seed);
        });
    }

    // -------------------------------------------------------------------------
    // Extra helpers for lifecycle
    // -------------------------------------------------------------------------

    public void clearCache() {
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
