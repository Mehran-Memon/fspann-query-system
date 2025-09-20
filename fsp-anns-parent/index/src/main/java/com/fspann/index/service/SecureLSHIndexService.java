package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.PartitioningPolicy;
import com.fspann.index.core.SecureLSHIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SecureLSHIndexService
 * -----------------------------------------------------------------------------
 * Unified entry point for indexing and lookup with two modes:

 *  1) partitioned (DEFAULT, paper-aligned):
 *     - Routes to a PaperSearchEngine (Coding → GreedyPartition → TagQuery).
 *     - Client-side kNN over small subset(s), forward-secure (re-encrypt only).
 *  2) multiprobe (legacy):
 *     - EvenLSH + SecureLSHIndex with per-table multi-probe bucket unions.
 *     - Retained for A/B tests and backwards compatibility.
 * Configure with: -Dfspann.mode=partitioned | multiprobe
  * Storage/crypto/lifecycle (RocksDB/AES-GCM/KeyService) are shared across modes.
 */
public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    // ----------------------------- Modes -----------------------------
    private static final String MODE = System.getProperty("fspann.mode", "partitioned");
    private static boolean isPartitioned() { return "partitioned".equalsIgnoreCase(MODE); }
    private static volatile java.lang.reflect.Field TABLES_F;

    // --------------------------- Core deps ---------------------------
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;

    // Legacy (multiprobe) optional test-injected components:
    private final SecureLSHIndex legacyIndex;     // may be null
    private final EvenLSH legacyLsh;              // may be null

    // Paper-aligned (partitioned) engine (inject your implementation; can be null)
    private volatile PaperSearchEngine paperEngine;
    public void setPaperEngine(PaperSearchEngine eng) { this.paperEngine = eng; }

    // Write buffer (shared across modes)
    private final EncryptedPointBuffer buffer;

    // Legacy defaults (used only in multiprobe mode)
    private final int defaultNumBuckets;
    private final int defaultNumTables;

    // Write-through toggle (persist to Rocks + metrics account)
    private volatile boolean writeThrough =
            !"false".equalsIgnoreCase(System.getProperty("index.writeThrough", "true"));
    public void setWriteThrough(boolean enabled) { this.writeThrough = enabled; }
    public boolean isWriteThrough() { return writeThrough; }

    // Legacy per-dimension contexts (multiprobe path only)
    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();

    // Small LRU of recently indexed points (for quick fetch/delete)
    private final Map<String, EncryptedPoint> indexedPoints =
            Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
                private static final int MAX = 200_000; // tune for your host
                @Override protected boolean removeEldestEntry(Map.Entry<String,EncryptedPoint> e) {
                    return size() > MAX;
                }
            });

    // -----------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------

    /**
     * Production constructor. Pass a PaperSearchEngine for the default paper mode.
     * If null, service seamlessly falls back to the legacy multiprobe path.
     */
    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager,
                                 PaperSearchEngine paperEngine,
                                 SecureLSHIndex legacyIndex,
                                 EvenLSH legacyLsh,
                                 EncryptedPointBuffer buffer,
                                 int defaultNumBuckets,
                                 int defaultNumTables) {
        this.crypto = (crypto != null) ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.paperEngine = paperEngine;
        this.legacyIndex = legacyIndex;
        this.legacyLsh = legacyLsh;
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.defaultNumBuckets = Math.max(1, defaultNumBuckets);
        this.defaultNumTables  = Math.max(1, defaultNumTables);
        logger.info("SecureLSHIndexService initialized in '{}' mode", MODE);
    }

    /** Legacy-compatible convenience constructor (no paper engine → multiprobe). */
    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager) {
        this(crypto, keyService, metadataManager,
                /*paperEngine*/ null,
                /*legacyIndex*/ null,
                /*legacyLsh*/ null,
                createBufferFromManager(metadataManager),
                /*defaultNumBuckets*/ 32,
                /*defaultNumTables*/ 4);
    }

    /** Legacy-style factory from SystemConfig. */
    public static SecureLSHIndexService fromConfig(CryptoService crypto,
                                                   KeyLifeCycleService keyService,
                                                   RocksDBMetadataManager metadata,
                                                   SystemConfig cfg) {
        int numBuckets  = Math.max(1, cfg.getNumShards());
        int numTables   = Math.max(1, cfg.getNumTables());
        EncryptedPointBuffer buf = createBufferFromManager(metadata);
        return new SecureLSHIndexService(crypto, keyService, metadata, null, null, null, buf, numBuckets, numTables);
    }

    private static EncryptedPointBuffer createBufferFromManager(RocksDBMetadataManager manager) {
        String pointsBase = Objects.requireNonNull(manager.getPointsBaseDir(),
                "metadataManager.getPointsBaseDir() returned null. In tests, stub or inject a buffer explicitly.");
        try {
            return new EncryptedPointBuffer(pointsBase, manager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    // -----------------------------------------------------------------
    // Legacy (multiprobe) helpers
    // -----------------------------------------------------------------

    private static long seedFor(int dim, int buckets, int projections) {
        long x = 0x9E3779B97F4A7C15L;
        x ^= (long) dim * 0xBF58476D1CE4E5B9L;
        x ^= (long) buckets * 0x94D049BB133111EBL;
        x ^= (long) projections + 0x2545F4914F6CDD1DL;
        x ^= (x >>> 33); x *= 0xff51afd7ed558ccdl;
        x ^= (x >>> 33); x *= 0xc4ceb9fe1a85ec53l;
        x ^= (x >>> 33);
        return x;
    }

    private DimensionContext getOrCreateLegacyContext(int dimension) {
        return dimensionContexts.computeIfAbsent(dimension, dim -> {
            int buckets = defaultNumBuckets;
            int projections = Math.max(1,
                    (int) Math.ceil(buckets * Math.log(Math.max(dim, 1) / 16.0) / Math.log(2)));

            long seed = seedFor(dim, buckets, projections);
            EvenLSH lshInstance = (this.legacyLsh != null)
                    ? this.legacyLsh
                    : new EvenLSH(dim, buckets, projections, seed);

            SecureLSHIndex idx = (this.legacyIndex != null)
                    ? this.legacyIndex
                    : new SecureLSHIndex(defaultNumTables, buckets, lshInstance);

            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }

    /** Exposed for tests/diagnostics in multiprobe mode. */
    public EvenLSH getLshForDimension(int dimension) { return getOrCreateLegacyContext(dimension).getLsh(); }

    // -----------------------------------------------------------------
    // IndexService API
    // -----------------------------------------------------------------

    public void batchInsert(List<String> ids, List<double[]> vectors) {
        if (ids == null || vectors == null || ids.size() != vectors.size()) {
            throw new IllegalArgumentException("IDs and vectors must be non-null and of equal size");
        }
        for (int i = 0; i < ids.size(); i++) {
            insert(ids.get(i), vectors.get(i));
        }
    }

    @Override
    public void insert(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        indexedPoints.put(pt.getId(), pt);

        if (isPartitioned() && paperEngine != null) {
            // Paper-aligned engines own placement via coding & partitions
            paperEngine.insert(pt);
        } else {
            // Legacy multiprobe
            int dimension = pt.getVectorLength();
            DimensionContext ctx = getOrCreateLegacyContext(dimension);
            ctx.getIndex().addPoint(pt);
        }

        // Write-through persistence (persist metadata first so tests can verify)
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
                return; // do not account failed writes
            }
            keyService.incrementOperation();

            // Best-effort buffer write (don’t let failures mask metadata persistence for tests)
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

        // Encrypt first (shared across modes)
        EncryptedPoint enc = crypto.encrypt(id, vector);

        // ---- Partitioned path (paper engine) ----
        if (isPartitioned() && paperEngine != null) {
            // Keep cache hot for quick fetch/delete
            indexedPoints.put(enc.getId(), enc);

            // Write-through persistence (persist metadata first so tests can verify)
            if (writeThrough) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put("version", String.valueOf(enc.getVersion()));
                metadata.put("dim", String.valueOf(vector.length));
                // (No per-table buckets at this stage for paper mode — that's OK)

                try {
                    metadataManager.batchUpdateVectorMetadata(
                            Collections.singletonMap(enc.getId(), metadata));
                    metadataManager.saveEncryptedPoint(enc);
                } catch (IOException e) {
                    logger.error("Failed to persist encrypted point {}", enc.getId(), e);
                    return; // do not account failed writes
                }
                keyService.incrementOperation();

                // Best-effort buffer write after metadata is persisted
                try {
                    buffer.add(enc);
                } catch (Exception e) {
                    logger.warn("Buffered write failed for {}", enc.getId(), e);
                }
            }

            // Hand placement to the paper engine (needs plaintext vector for coding)
            paperEngine.insert(enc, vector);
            return;
        }

        // ---- Legacy multiprobe path ----
        int dimension = vector.length;
        DimensionContext ctx = getOrCreateLegacyContext(dimension);
        SecureLSHIndex idx = ctx.getIndex();

        // Compute per-table bucket ids
        List<Integer> perTableBuckets = new ArrayList<>(idx.getNumHashTables());
        for (int t = 0; t < idx.getNumHashTables(); t++) {
            perTableBuckets.add(ctx.getLsh().getBucketId(vector, t));
        }

        // Build legacy-style EncryptedPoint carrying bucket tags
        EncryptedPoint ep = new EncryptedPoint(
                enc.getId(),
                perTableBuckets.get(0), // legacy shard/bucket field
                enc.getIv(),
                enc.getCiphertext(),
                enc.getVersion(),
                vector.length,
                perTableBuckets
        );

        // Delegate to the common insert(pt) which also handles write-through
        insert(ep);
    }

    @Override
    public LookupWithDiagnostics lookupWithDiagnostics(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");

        // ---- Partitioned path (paper engine) ----
        if (isPartitioned() && paperEngine != null) {
            List<EncryptedPoint> cands = paperEngine.lookup(token);
            // For now, we don’t have per-division fanout here; use size only.
            SearchDiagnostics diag = new SearchDiagnostics(
                    (cands != null) ? cands.size() : 0,
                    0,
                    java.util.Map.of()
            );
            return new LookupWithDiagnostics((cands != null) ? cands : java.util.List.of(), diag);
        }

        // ---- Legacy multiprobe path ----
        final int dim = token.getDimension();
        final DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) {
            return new LookupWithDiagnostics(java.util.List.of(), SearchDiagnostics.EMPTY);
        }
        final SecureLSHIndex idx = ctx.getIndex();

        // Resolve per-table expansions
        List<List<Integer>> perTable = token.hasPerTable()
                ? token.getTableBuckets()
                : com.fspann.index.core.PartitioningPolicy.expansionsForQuery(
                ctx.getLsh(), token.getPlaintextQuery(), idx.getNumHashTables(), token.getTopK());

        // Materialize a mutable copy if needed (token buckets may be unmodifiable)
        if (token.hasPerTable()) {
            List<List<Integer>> cp = new ArrayList<>(perTable.size());
            for (List<Integer> l : perTable) cp.add(new ArrayList<>(l));
            perTable = cp;
        }

        // Build union + track diagnostics (fanout per table, probed bucket count)
        final int tablesToUse = Math.min(token.getNumTables(), idx.getNumHashTables());
        final Map<Integer, Integer> fanoutPerTable = new LinkedHashMap<>();
        final Set<String> seen = new LinkedHashSet<>(16_384);
        final List<EncryptedPoint> ordered = new ArrayList<>();
        int probedBuckets = 0;

        for (int t = 0; t < tablesToUse; t++) {
            int contributed = 0;

            for (Integer b : perTable.get(t)) {
                probedBuckets++;
                // Access bucket list via a lightweight accessor:
                // Small inline: mirrored from SecureLSHIndex#queryEncrypted
                java.util.concurrent.CopyOnWriteArrayList<EncryptedPoint> bucket =
                        getBucketList(idx, t, b); // helper below
                if (bucket == null) continue;
                for (EncryptedPoint pt : bucket) {
                    if (pt.getId() != null && pt.getId().startsWith("FAKE_")) continue;
                    if (seen.add(pt.getId())) {
                        ordered.add(pt);
                        contributed++;
                    }
                }
            }
            fanoutPerTable.put(t, contributed);
        }

        SearchDiagnostics diag = new SearchDiagnostics(seen.size(), probedBuckets, java.util.Map.copyOf(fanoutPerTable));
        return new LookupWithDiagnostics(ordered, diag);
    }

    @SuppressWarnings("unchecked")
    private static java.util.concurrent.CopyOnWriteArrayList<EncryptedPoint> getBucketList(SecureLSHIndex idx, int tableId, int bucketId) {
        try {
            java.lang.reflect.Field f = TABLES_F;
            if (f == null) {
                f = SecureLSHIndex.class.getDeclaredField("tables");
                f.setAccessible(true);
                TABLES_F = f;
            }
            var tables = (java.util.List<java.util.Map<Integer, java.util.concurrent.CopyOnWriteArrayList<EncryptedPoint>>>) f.get(idx);
            if (tableId < 0 || tableId >= tables.size()) return null;
            return tables.get(tableId).get(bucketId);
        } catch (Throwable ignore) {
            return null;
        }
    }


    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        return lookupWithDiagnostics(token).candidates();
    }

    private static boolean canShrink(List<List<Integer>> perTable) {
        for (List<Integer> t : perTable) if (t.size() > 1) return true;
        return false;
    }

    private static void shrinkWorstTail(List<List<Integer>> perTable) {
        for (List<Integer> t : perTable) {
            int n = t.size();
            if (n > 1) t.remove(n - 1);
        }
    }

    @Override
    public void delete(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        indexedPoints.remove(id);

        // Paper engine owns its structures in partitioned mode
        if (isPartitioned() && paperEngine != null) {
            paperEngine.delete(id);
            return;
        }

        // Legacy multiprobe cleanup
        EncryptedPoint pt = getEncryptedPoint(id);
        if (pt != null) {
            DimensionContext ctx = dimensionContexts.get(pt.getVectorLength());
            if (ctx != null) ctx.getIndex().removePoint(id);
            else logger.warn("No legacy context for dimension {} during delete", pt.getVectorLength());
        }
    }

    private Map<String, Map<String, String>> fetchMetadata(List<EncryptedPoint> points) {
        Map<String, Map<String, String>> out = new HashMap<>(points.size());
        for (EncryptedPoint p : points) out.put(p.getId(), metadataManager.getVectorMetadata(p.getId()));
        return out;
    }

    @Override public void markDirty(int shardId) { /* no-op */ }

    @Override public int getIndexedVectorCount() { return indexedPoints.size(); }

    @Override public Set<Integer> getRegisteredDimensions() { return dimensionContexts.keySet(); }

    @Override
    public int getVectorCountForDimension(int dimension) {
        if (isPartitioned() && paperEngine != null) return paperEngine.getVectorCountForDimension(dimension);
        DimensionContext ctx = dimensionContexts.get(dimension);
        return (ctx == null) ? 0 : ctx.getIndex().getPointCount();
    }

    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        EncryptedPoint cached = indexedPoints.get(id);
        if (cached != null) return cached;
        try {
            return metadataManager.loadEncryptedPoint(id);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to load encrypted point {} from disk", id, e);
            return null;
        }
    }

    public void updateCachedPoint(EncryptedPoint pt) { indexedPoints.put(pt.getId(), pt); }

    public void flushBuffers() { buffer.flushAll(); }

    @Override public EncryptedPointBuffer getPointBuffer() { return buffer; }

    @Override
    public int getShardIdForVector(double[] vector) {
        if (isPartitioned()) return -1; // diagnostic is meaningless in partitioned mode
        int dim = Objects.requireNonNull(vector, "vector").length;
        DimensionContext ctx = getOrCreateLegacyContext(dim);
        return ctx.getLsh().getBucketId(vector, 0);
    }

    public void clearCache() {
        int size = indexedPoints.size();
        indexedPoints.clear();
        logger.info("Cleared {} cached points", size);
    }

    // Legacy eval helper (multiprobe only). In paper mode, prefer recall@k vs exact-k baselines.
    public Map<Integer, Double> evaluateFanoutRatio(double[] query) {
        return evaluateFanoutRatio(query, new int[]{1, 20, 40, 60, 80, 100});
    }

    public Map<Integer, Double> evaluateFanoutRatio(double[] query, int[] topKs) {
        Objects.requireNonNull(query, "query");
        if (isPartitioned()) return Collections.emptyMap();

        DimensionContext ctx = getOrCreateLegacyContext(query.length);
        SecureLSHIndex idx = ctx.getIndex();
        Map<Integer, Double> out = new LinkedHashMap<>();
        int N = Math.max(1, idx.getPointCount());

        for (int k : topKs) {
            List<List<Integer>> perTable = PartitioningPolicy
                    .expansionsForQuery(ctx.getLsh(), query, idx.getNumHashTables(), k);
            int cand = idx.candidateCount(perTable);
            out.put(k, cand / (double) N);
        }
        return out;
    }

    public void addToIndexOnly(String id, double[] vec) {
        boolean prev = writeThrough;
        writeThrough = false;
        try { insert(id, vec); } finally { writeThrough = prev; }
    }

    public void addPointToIndexOnly(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint");
        indexedPoints.put(pt.getId(), pt);
        if (isPartitioned() && paperEngine != null) paperEngine.insert(pt);
        else getOrCreateLegacyContext(pt.getVectorLength()).getIndex().addPoint(pt);
    }

    @Override
    public int candidateCount(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");

        if (isPartitioned() && paperEngine != null) {
            // Paper engines may override with more precise accounting
            List<EncryptedPoint> cands = paperEngine.lookup(token);
            return (cands != null) ? cands.size() : 0;
        }

        // Legacy multiprobe path
        int dim = token.getDimension();
        DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) return 0;
        SecureLSHIndex idx = ctx.getIndex();

        List<List<Integer>> perTable = token.hasPerTable()
                ? token.getTableBuckets()
                : PartitioningPolicy.expansionsForQuery(
                ctx.getLsh(),
                token.getPlaintextQuery(),
                idx.getNumHashTables(),
                token.getTopK());

        return idx.candidateCount(perTable);
    }

    public void shutdown() { buffer.shutdown(); }

    // -----------------------------------------------------------------
    // Paper-aligned engine contract (inject your implementation)
    // -----------------------------------------------------------------
    /**
     * PaperSearchEngine abstracts the paper-aligned pipeline:
     *  - Coding (Algorithm-1)
     *  - Greedy partition + map index I (Algorithm-2)
     *  - Tag query + subset retrieval + client kNN (Algorithm-3)
     *
     * Implementations must:
     *  - Persist G, I, tag→subset mapping, and w
     *  - Preserve forward-security when rotating keys (re-encrypt; keep tags)
     *  - Exclude fake points from evaluation/candidate sets
     */
    public interface PaperSearchEngine {
        void insert(EncryptedPoint pt);                       // encrypted only
        void insert(EncryptedPoint pt, double[] plaintextVector); // with vector for coding
        List<EncryptedPoint> lookup(QueryToken token);        // returns encrypted candidates (re-rank inside engine)
        void delete(String id);
        int getVectorCountForDimension(int dimension);
    }
}
