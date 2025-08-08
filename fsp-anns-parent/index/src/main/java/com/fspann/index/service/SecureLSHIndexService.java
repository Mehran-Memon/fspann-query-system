package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.PartitioningPolicy;
import com.fspann.index.core.SecureLSHIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fspann.common.QueryToken;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    private static final int DEFAULT_NUM_BUCKETS = 32;

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;
    private final SecureLSHIndex index;   // optional injected single-index (tests)
    private final EvenLSH lsh;            // optional injected LSH (tests)
    private final EncryptedPointBuffer buffer;
    private static final int DEFAULT_NUM_TABLES = 4; // or 8; tune later

    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final Map<String, EncryptedPoint> indexedPoints = new ConcurrentHashMap<>();

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager) {
        this(crypto, keyService, metadataManager, null, null, createBufferFromManager(metadataManager));
    }

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager,
                                 SecureLSHIndex index,
                                 EvenLSH lsh,
                                 EncryptedPointBuffer buffer) {
        this.crypto = (crypto != null) ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.index = index;
        this.lsh = lsh;
        this.buffer = Objects.requireNonNull(buffer, "buffer");
    }

    private static EncryptedPointBuffer createBufferFromManager(RocksDBMetadataManager manager) {
        String pointsBase = Objects.requireNonNull(manager.getPointsBaseDir(),
                "metadataManager.getPointsBaseDir() returned null. " +
                        "In tests, stub this or inject a buffer explicitly.");
        try {
            return new EncryptedPointBuffer(pointsBase, manager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    private DimensionContext getOrCreateContext(int dimension) {
        return dimensionContexts.computeIfAbsent(dimension, dim -> {
            int buckets = Math.max(1, DEFAULT_NUM_BUCKETS);
            int projections = Math.max(1,
                    (int) Math.ceil(buckets * Math.log(Math.max(dim, 1) / 16.0) / Math.log(2)));

            long seed = seedFor(dim, buckets, projections); // <--- deterministic
            EvenLSH lshInstance = (this.lsh != null)
                    ? this.lsh
                    : new EvenLSH(dim, buckets, projections, seed);

            SecureLSHIndex idx = (this.index != null)
                    ? this.index
                    : new SecureLSHIndex(DEFAULT_NUM_TABLES, buckets, lshInstance);

            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }


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

    /** Batch inserts raw vectors with IDs. */
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
        int dimension = pt.getVectorLength();
        DimensionContext ctx = getOrCreateContext(dimension);

        indexedPoints.put(pt.getId(), pt);
        SecureLSHIndex idx = ctx.getIndex();
        idx.addPoint(pt);
        buffer.add(pt);

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
            return; // don't count failed writes
        }

        // <-- rotation accounting
        keyService.incrementOperation();
    }

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");

        int dimension = vector.length;
        DimensionContext ctx = getOrCreateContext(dimension);
        SecureLSHIndex idx = ctx.getIndex();

        EncryptedPoint enc = crypto.encrypt(id, vector);

        List<Integer> perTableBuckets = new ArrayList<>(idx.getNumHashTables());
        for (int t = 0; t < idx.getNumHashTables(); t++) {
            perTableBuckets.add(ctx.getLsh().getBucketId(vector, t));
        }

        EncryptedPoint ep = new EncryptedPoint(
                enc.getId(),
                perTableBuckets.get(0), // legacy field; not used semantically
                enc.getIv(),
                enc.getCiphertext(),
                enc.getVersion(),
                vector.length,
                perTableBuckets
        );

        insert(ep); // insert() calls incrementOperation() after persistence
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        int dim = token.getDimension();
        DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) {
            logger.warn("No index for dimension {}", dim);
            return Collections.emptyList();
        }
        SecureLSHIndex idx = ctx.getIndex();

        // Ensure per-table expansions exist
        final List<List<Integer>> perTable;
        if (token.hasPerTable()) {
            perTable = token.getTableBuckets();
        } else {
            // derive expansions from legacy buckets / query vector
            perTable = PartitioningPolicy.expansionsForQuery(
                    ctx.getLsh(),
                    token.getPlaintextQuery(),
                    idx.getNumHashTables(),
                    token.getTopK()
            );
        }

        // Build a per-table token for the index (re-using all metadata)
        QueryToken perTableToken = new QueryToken(
                perTable,
                token.getIv(),
                token.getEncryptedQuery(),
                token.getPlaintextQuery(),
                token.getTopK(),
                idx.getNumHashTables(),
                token.getEncryptionContext(),
                token.getDimension(),
                token.getVersion()
        );

        List<EncryptedPoint> candidates = idx.queryEncrypted(perTableToken);
        if (candidates.isEmpty()) return Collections.emptyList();

        // Metadata filter: version & dimension (kept from previous behavior)
        Map<String, Map<String, String>> metas = fetchMetadata(candidates);
        List<EncryptedPoint> filtered = new ArrayList<>(candidates.size());
        for (EncryptedPoint pt : candidates) {
            Map<String, String> m = metas.get(pt.getId());
            if (m != null
                    && Integer.toString(pt.getVersion()).equals(m.get("version"))
                    && Integer.toString(pt.getVectorLength()).equals(m.get("dim"))) {
                filtered.add(pt);
            }
        }
        return filtered;
    }

    @Override
    public void delete(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        EncryptedPoint pt = indexedPoints.remove(id);
        if (pt != null) {
            DimensionContext ctx = dimensionContexts.get(pt.getVectorLength());
            if (ctx != null) {
                ctx.getIndex().removePoint(id);
            } else {
                logger.warn("No context found for dimension {} during delete", pt.getVectorLength());
            }
        }
    }

    private Map<String, Map<String, String>> fetchMetadata(List<EncryptedPoint> points) {
        Map<String, Map<String, String>> out = new HashMap<>(points.size());
        for (EncryptedPoint p : points) {
            out.put(p.getId(), metadataManager.getVectorMetadata(p.getId()));
        }
        return out;
    }

    // Legacy no-op (kept to satisfy IndexService, but no shard semantics now)
    @Override
    public void markDirty(int shardId) { /* no-op in proposal-aligned design */ }

    @Override
    public int getIndexedVectorCount() { return indexedPoints.size(); }

    @Override
    public Set<Integer> getRegisteredDimensions() { return dimensionContexts.keySet(); }

    @Override
    public int getVectorCountForDimension(int dimension) {
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

    @Override
    public EncryptedPointBuffer getPointBuffer() { return buffer; }

    @Override
    public int getShardIdForVector(double[] vector) {
        int dim = Objects.requireNonNull(vector, "vector").length;
        DimensionContext ctx = getOrCreateContext(dim);
        return ctx.getLsh().getBucketId(vector, 0); // legacy diagnostic only
    }

    public void clearCache() {
        int size = indexedPoints.size();
        indexedPoints.clear();
        logger.info("Cleared {} cached points", size);
    }

    public Map<Integer, Double> evaluateFanoutRatio(double[] query) {
        return evaluateFanoutRatio(query, new int[]{1, 20, 40, 60, 80, 100});
    }

    public Map<Integer, Double> evaluateFanoutRatio(double[] query, int[] topKs) {
        Objects.requireNonNull(query, "query");
        DimensionContext ctx = getOrCreateContext(query.length);
        SecureLSHIndex idx = ctx.getIndex();
        Map<Integer, Double> out = new LinkedHashMap<>();
        int N = Math.max(1, idx.getPointCount()); // avoid div-by-zero

        for (int k : topKs) {
            List<List<Integer>> perTable = PartitioningPolicy
                    .expansionsForQuery(ctx.getLsh(), query, idx.getNumHashTables(), k);
            int cand = idx.candidateCount(perTable);
            double ratio = cand / (double) N;
            out.put(k, ratio); // <--- populate
        }
        return out;
    }

    public void shutdown() { buffer.shutdown(); }
}
