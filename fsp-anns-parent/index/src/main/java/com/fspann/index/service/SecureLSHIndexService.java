package com.fspann.index.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.IndexService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    private static final int DEFAULT_NUM_SHARDS = 32;

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;
    private final SecureLSHIndex index;   // optional injected single-index (used by tests)
    private final EvenLSH lsh;            // optional injected LSH (used by tests)
    private final EncryptedPointBuffer buffer;

    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final Map<String, EncryptedPoint> indexedPoints = new ConcurrentHashMap<>();

    /**
     * Default ctor: builds a buffer using the metadata manager's base dir (no hard-coded paths).
     * In production, prefer creating the buffer in the API layer and injecting via the 6-arg ctor,
     * so this module does not create any directories on its own.
     */
    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager) {
        this(crypto, keyService, metadataManager, null, null, createBufferFromManager(metadataManager));
    }

    /**
     * Fully-injected ctor. If you want the index module to avoid creating directories,
     * pass a prebuilt buffer from the API layer here.
     */
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
            // Use the manager's base dir. If you want zero directory creation in this module,
            // inject the buffer via the 6-arg constructor instead.
            return new EncryptedPointBuffer(pointsBase, manager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    private DimensionContext getOrCreateContext(int dimension) {
        return dimensionContexts.computeIfAbsent(dimension, dim -> {
            int buckets = Math.max(1, DEFAULT_NUM_SHARDS);
            int projections = Math.max(1, (int) Math.ceil(buckets * Math.log(Math.max(dim, 1) / 16.0) / Math.log(2)));
            EvenLSH lshInstance = (this.lsh != null) ? this.lsh : new EvenLSH(dim, buckets, projections);
            SecureLSHIndex idx = (this.index != null) ? this.index : new SecureLSHIndex(1, buckets, lshInstance);
            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }

    /** Batch inserts a list of raw vectors with corresponding IDs. */
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
        idx.markShardDirty(pt.getShardId());
        buffer.add(pt);

        // persist metadata & point (batch for metadata to match tests)
        Map<String, String> metadata = new HashMap<>();
        metadata.put("shardId", String.valueOf(pt.getShardId()));
        metadata.put("version", String.valueOf(pt.getVersion()));
        metadata.put("dim", String.valueOf(pt.getVectorLength()));

        try {
            metadataManager.batchUpdateVectorMetadata(Collections.singletonMap(pt.getId(), metadata));
            metadataManager.saveEncryptedPoint(pt);
        } catch (IOException e) {
            logger.error("Failed to persist encrypted point {}", pt.getId(), e);
        }
    }

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        int dimension = vector.length;
        DimensionContext ctx = getOrCreateContext(dimension);

        EncryptedPoint encryptedPoint = crypto.encrypt(id, vector);
        int shardId = ctx.getLsh().getBucketId(vector);

        EncryptedPoint withShard = new EncryptedPoint(
                encryptedPoint.getId(),
                shardId,
                encryptedPoint.getIv(),
                encryptedPoint.getCiphertext(),
                encryptedPoint.getVersion(),
                vector.length,
                Collections.singletonList(shardId)
        );

        insert(withShard);
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

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        int dim = token.getQueryVector().length;
        DimensionContext ctx = dimensionContexts.get(dim);
        if (ctx == null) {
            logger.warn("No index for dimension {}", dim);
            return Collections.emptyList();
        }

        List<EncryptedPoint> candidates = ctx.getIndex().queryEncrypted(token);
        if (candidates.isEmpty()) return Collections.emptyList();

        // fetch metadata per id (since multiGet isn't available on your manager)
        Map<String, Map<String, String>> metas = fetchMetadata(candidates);

        List<EncryptedPoint> filtered = new ArrayList<>(candidates.size());
        for (EncryptedPoint pt : candidates) {
            Map<String, String> m = metas.get(pt.getId());
            if (m != null
                    && Integer.toString(pt.getVersion()).equals(m.get("version"))
                    && Integer.toString(pt.getShardId()).equals(m.get("shardId"))
                    && Integer.toString(pt.getVectorLength()).equals(m.get("dim"))) {
                filtered.add(pt);
            }
        }
        return filtered;
    }

    private Map<String, Map<String, String>> fetchMetadata(List<EncryptedPoint> points) {
        Map<String, Map<String, String>> out = new HashMap<>(points.size());
        for (EncryptedPoint p : points) {
            out.put(p.getId(), metadataManager.getVectorMetadata(p.getId()));
        }
        return out;
    }

    @Override
    public void markDirty(int shardId) {
        for (DimensionContext ctx : dimensionContexts.values()) {
            ctx.getIndex().markShardDirty(shardId);
        }
    }

    @Override
    public int getIndexedVectorCount() {
        return indexedPoints.size();
    }

    @Override
    public Set<Integer> getRegisteredDimensions() {
        return dimensionContexts.keySet();
    }

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

    public void updateCachedPoint(EncryptedPoint pt) {
        indexedPoints.put(pt.getId(), pt);
    }

    public void flushBuffers() {
        buffer.flushAll();
    }

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return buffer;
    }

    @Override
    public int getShardIdForVector(double[] vector) {
        int dim = Objects.requireNonNull(vector, "vector").length;
        DimensionContext ctx = getOrCreateContext(dim);
        return ctx.getLsh().getBucketId(vector);
    }

    public void clearCache() {
        int size = indexedPoints.size();
        indexedPoints.clear();
        logger.info("Cleared {} cached points", size);
    }

    public void shutdown() {
        buffer.shutdown();
    }
}
