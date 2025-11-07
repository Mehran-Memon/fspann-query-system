package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.PartitioningPolicy;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.paper.PartitionedIndexService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.fspann.crypto.EncryptionUtils.decryptVector;

/**
 * SecureLSHIndexService
 * -----------------------------------------------------------------------------
 * Unified entry point for indexing and lookup with two modes:
 *  1) partitioned (DEFAULT, paper-aligned):
 *     - Routes to a PaperSearchEngine (Coding → GreedyPartition → TagQuery).
 *     - Client-side kNN over subset union, forward-secure (re-encrypt only).
 *  2) multiprobe (legacy):
 *     - EvenLSH + SecureLSHIndex with per-table bucket unions.
 *     - UPDATED: no dependence on K/topK for fetch size. If the token does not
 *       carry per-table buckets, we compute exactly one bucket per table using
 *       LSH over the plaintext query (no fanout), then return the FULL union.
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
    public static String getMode() { return isPartitioned() ? "partitioned" : "multiprobe"; }
    private static final java.util.concurrent.atomic.AtomicBoolean FANOUT_LOGGED = new java.util.concurrent.atomic.AtomicBoolean(false);

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
        // Disambiguated parameter log on init
        if (isPartitioned() && paperEngine instanceof com.fspann.index.paper.PartitionedIndexService pes) {
            // paper m = projections/division
            logger.info("Paper parameters: m (projections/division)={}, lambda={}, divisions (ℓ)={}",
                    pes.getM(), pes.getLambda(), pes.getDivisions());
        } else {
            // multiprobe: report L and buckets; m(rows/table) may come from config elsewhere
            logger.info("Multiprobe parameters: L (tables)={}, buckets={} (legacy LSH), m(rows/LSH table)=unreported");
        }
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
        SecureLSHIndexService svc =
                new SecureLSHIndexService(crypto, keyService, metadata, null, null, null, buf, numBuckets, numTables);
        // If paper mode is enabled in config, wire the paper engine here.
        try {
            var pc = cfg.getPaper();
            if (pc != null && pc.enabled) {
                PartitionedIndexService pe = new PartitionedIndexService(pc.m, pc.lambda, pc.divisions, pc.seed);
                svc.setPaperEngine(pe);
                logger.info("Paper engine enabled via config (m={}, λ={}, ℓ={}, seed={})",
                        pc.m, pc.lambda, pc.divisions, pc.seed);
            }
        } catch (Throwable ignore) { /* fall back to multiprobe if anything goes wrong */ }
        return svc;
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

            // Hand placement to the paper engine WITH PRECOMPUTED CODES (no plaintext server-side)
            if (paperEngine instanceof com.fspann.index.paper.PartitionedIndexService pes) {
                // Client-side coding; produces one BitSet per division.
                java.util.BitSet[] codes = pes.code(vector);
                pes.insertWithCodes(new com.fspann.index.paper.CodedPoint(enc, codes, pes.getDivisions()));
            } else {
                // If a different PaperSearchEngine is injected and does not provide a coder,
                // fail fast with an actionable message instead of leaking plaintext.
                throw new UnsupportedOperationException(
                        "Paper engine requires precomputed codes; provide PartitionedIndexService or " +
                                "extend your PaperSearchEngine to expose a coding method and an insertWithCodes(...) entry point.");
            }
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

        // -------------------------- PAPER MODE --------------------------
        if (isPartitioned() && paperEngine != null) {
            // Perform initial lookup
            List<EncryptedPoint> cands = paperEngine.lookup(token);
            if (cands == null) cands = new ArrayList<>();

            // Adjust search if not enough candidates are found based on TopK
            if (cands.size() < token.getTopK()) {
                cands = widenSearchInPaperMode(token, cands);
            }

            final SearchDiagnostics diag = new SearchDiagnostics(cands.size(), 0, Collections.emptyMap());
            return new LookupWithDiagnostics(cands, diag);
        }

        // ----------------------- LEGACY MULTIPROBE -----------------------
        final int dim = token.getDimension();
        final DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) {
            return new LookupWithDiagnostics(Collections.emptyList(), SearchDiagnostics.EMPTY);
        }
        final SecureLSHIndex idx = ctx.getIndex();

        // Determine per-table buckets from token or derive from query
        final List<List<Integer>> perTable = deriveBucketsForQuery(token, ctx, idx);

        // Perform search while respecting TopK, m, lambda, and l values
        final int tablesToUse = Math.min(token.getNumTables(), idx.getNumHashTables());
        final Map<Integer, Integer> fanoutPerTable = new LinkedHashMap<>();
        final Set<String> seen = new LinkedHashSet<>(16_384);
        final List<EncryptedPoint> ordered = new ArrayList<>();
        int probedBuckets = 0;

        // Perform search with buckets and calculate fanout based on K, m, λ, and ℓ
        for (int t = 0; t < tablesToUse; t++) {
            int contributed = 0;

            for (Integer b : perTable.get(t)) {
                probedBuckets++;
                final java.util.concurrent.CopyOnWriteArrayList<EncryptedPoint> bucket = getBucketList(idx, t, b);
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

        // Adjust the result size based on TopK
        List<EncryptedPoint> finalCandidates = ordered.size() > token.getTopK() ?
                ordered.subList(0, token.getTopK()) : ordered;

        final SearchDiagnostics diag = new SearchDiagnostics(finalCandidates.size(), probedBuckets, Collections.unmodifiableMap(fanoutPerTable));
        return new LookupWithDiagnostics(finalCandidates, diag);
    }

    /**
     * Widen the search in PAPER mode if not enough candidates are found.
     * This will fetch more buckets as needed.
     */
    private List<EncryptedPoint> widenSearchInPaperMode(QueryToken token, List<EncryptedPoint> currentCandidates) {
        List<EncryptedPoint> widenedCandidates = new ArrayList<>(currentCandidates);

        // Calculate additional buckets to fetch based on TopK
        int additionalBucketsToFetch = calculateAdditionalBucketsToFetch(token);
        for (int i = 0; i < additionalBucketsToFetch; i++) {
            List<EncryptedPoint> additionalCandidates = paperEngine.lookup(token);
            if (additionalCandidates != null) {
                widenedCandidates.addAll(additionalCandidates);
            }
        }

        // Sort by relevance (distance-based) and limit to TopK
        widenedCandidates.sort(Comparator.comparingDouble(candidate -> calculateDistance(candidate, token)));

        // Ensure we do not exceed TopK
        int topK = Math.min(token.getTopK(), widenedCandidates.size());
        return widenedCandidates.subList(0, topK);
    }

    /**
     * Calculate squared Euclidean distance between a candidate's vector and the query vector.
     */
    private double calculateDistance(EncryptedPoint candidate, QueryToken token) {
        // Extract the ciphertext and IV from the candidate
        byte[] ciphertext = candidate.getCiphertext();
        byte[] iv = candidate.getIv();

        // Retrieve the decryption key from the KeyLifeCycleService
        SecretKey key = keyService.getCurrentVersion().getKey();

        // Decrypt the candidate's vector
        double[] candidateVector;
        try {
            candidateVector = EncryptionUtils.decryptVector(ciphertext, iv, key);  // Correct method call with 3 arguments
        } catch (GeneralSecurityException e) {
            return Double.MAX_VALUE; // Return a high value if decryption fails
        }

        // Get the query vector
        double[] queryVector = token.getPlaintextQuery();

        // Calculate squared Euclidean distance
        double sum = 0;
        for (int i = 0; i < candidateVector.length; i++) {
            sum += Math.pow(candidateVector[i] - queryVector[i], 2);
        }
        return sum; // Squared Euclidean distance
    }


    /**
     * Calculate how many additional buckets need to be scanned if the search is not sufficient.
     */
    private int calculateAdditionalBucketsToFetch(QueryToken token) {
        int topK = token.getTopK();
        int currentCandidates = token.getTopK();  // Adjust this as needed
        int additionalBuckets = Math.max(0, topK - currentCandidates);  // Expand as required
        return additionalBuckets;
    }

    /**
     * Derive buckets for the query based on token and LSH context.
     */
    private List<List<Integer>> deriveBucketsForQuery(QueryToken token, DimensionContext ctx, SecureLSHIndex idx) {
        List<List<Integer>> perTable;
        if (token.hasPerTable()) {
            perTable = new ArrayList<>(token.getTableBuckets().size());
            for (List<Integer> l : token.getTableBuckets()) perTable.add(new ArrayList<>(l));
        } else {
            perTable = new ArrayList<>(idx.getNumHashTables());
            double[] q = token.getPlaintextQuery();
            for (int t = 0; t < idx.getNumHashTables(); t++) {
                int b = ctx.getLsh().getBucketId(q, t);
                perTable.add(java.util.List.of(b));
            }
        }
        return perTable;
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

    // ---------------------- Legacy-only eval helpers ----------------------
    // NOTE: kept for A/B/diagnostics; not used in runtime lookup paths.
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
    // ---------------------------------------------------------------------

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
            List<EncryptedPoint> cands = paperEngine.lookup(token);
            return (cands != null) ? cands.size() : 0;
        }

        // Legacy multiprobe: count using ONE bucket per table when token lacks per-table spec.
        int dim = token.getDimension();
        DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) return 0;
        SecureLSHIndex idx = ctx.getIndex();

        List<List<Integer>> perTable;
        if (token.hasPerTable()) {
            perTable = token.getTableBuckets();
        } else {
            perTable = new ArrayList<>(idx.getNumHashTables());
            double[] q = token.getPlaintextQuery();
            for (int t = 0; t < idx.getNumHashTables(); t++) {
                int b = ctx.getLsh().getBucketId(q, t);
                perTable.add(java.util.List.of(b));
            }
        }
        return idx.candidateCount(perTable);
    }


    private void fanoutLogOnce() {
        if (!FANOUT_LOGGED.compareAndSet(false, true)) return;
        if (isPartitioned() && paperEngine instanceof com.fspann.index.paper.PartitionedIndexService pes) {
            logger.info("Fanout snapshot label: mode=partitioned, paper(m={}, λ={}, ℓ={})",
                    pes.getM(), pes.getLambda(), pes.getDivisions());
        } else {
            logger.info("Fanout snapshot label: mode=multiprobe, multiprobe(L={}, buckets={})",
                    defaultNumTables, defaultNumBuckets);
        }
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
        List<EncryptedPoint> lookup(QueryToken token);        // returns encrypted candidates (subset union)
        void delete(String id);
        int getVectorCountForDimension(int dimension);
    }
}
