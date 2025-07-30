package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.core.EvenLSH;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;
    private final SecureLSHIndex index;
    private final EvenLSH lsh;
    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final Map<String, EncryptedPoint> indexedPoints = new ConcurrentHashMap<>();
    private final EncryptedPointBuffer buffer;
    private static final int DEFAULT_NUM_SHARDS = 32;

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager) {
        this(crypto, keyService, metadataManager, null, null, createBuffer(metadataManager));
    }

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadataManager,
                                 SecureLSHIndex index,
                                 EvenLSH lsh,
                                 EncryptedPointBuffer buffer) {
        this.crypto = crypto != null ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = keyService;
        this.metadataManager = metadataManager;
        this.index = index;
        this.lsh = lsh;
        this.buffer = buffer;
    }

    private static EncryptedPointBuffer createBuffer(RocksDBMetadataManager metadataManager) {
        try {
            return new EncryptedPointBuffer("metadata/points", metadataManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    private DimensionContext getOrCreateContext(int dimension) {
        return dimensionContexts.computeIfAbsent(dimension, dim -> {
            int buckets = Math.max(1, DEFAULT_NUM_SHARDS);
            int projections = Math.max(1, (int) Math.ceil(buckets * Math.log(Math.max(dim, 1) / 16.0) / Math.log(2)));
            EvenLSH lshInstance = (lsh != null) ? lsh : new EvenLSH(dim, Math.max(1, buckets), Math.max(1, projections));
            SecureLSHIndex idx = (index != null) ? index : new SecureLSHIndex(1, buckets, lshInstance);
            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }

    /**
     * Batch inserts a list of raw vectors with corresponding IDs.
     */
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
        try {
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

        try {
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
        } catch (AesGcmCryptoService.CryptoException e) {
            logger.error("Failed to encrypt vector id={}", id, e);
            throw new RuntimeException("Encryption failed for id=" + id, e);
        }
    }

    @Override
    public void delete(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        EncryptedPoint pt = indexedPoints.remove(id);
        if (pt != null) {
            int dimension = pt.getVectorLength();
            DimensionContext ctx = dimensionContexts.get(dimension);
            if (ctx == null) {
                logger.warn("No context found for dimension {} during delete", dimension);
                return;
            }
            ctx.getIndex().removePoint(id);
        }
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        int dimension = token.getQueryVector().length;
        DimensionContext ctx = dimensionContexts.get(dimension);
        if (ctx == null) {
            logger.warn("No index found for dimension: {}", dimension);
            return Collections.emptyList();
        }
        return ctx.getIndex().queryEncrypted(token);
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
        if (ctx == null) return 0;
        return ctx.getIndex().getPointCount();
    }

    public int getShardIdForVector(double[] vector) {
        Objects.requireNonNull(vector, "Vector cannot be null");
        int dim = vector.length;
        DimensionContext ctx = getOrCreateContext(dim);
        return ctx.getLsh().getBucketId(vector);
    }

    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
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
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        indexedPoints.put(pt.getId(), pt);
    }

    public void flushBuffers() {
        buffer.flushAll();
    }

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return buffer;
    }

    public void clearCache() {
        int size = indexedPoints.size();
        indexedPoints.clear();
        logger.info("Cleared {} cached points", size);
    }

    public void shutdown() {
        buffer.shutdown();
        metadataManager.close();
    }
}
