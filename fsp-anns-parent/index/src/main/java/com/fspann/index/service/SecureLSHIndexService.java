package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.core.EvenLSH;
import com.fspann.common.RocksDBMetadataManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
    private final Map<String, Map<String, String>> pendingMetadata = new ConcurrentHashMap<>();
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
            int buckets = DEFAULT_NUM_SHARDS;
            int projections = (int) Math.ceil(buckets * Math.log(Math.max(dim, 1) / 16.0) / Math.log(2));
            EvenLSH lshInstance = (lsh != null) ? lsh : new EvenLSH(dim, buckets, projections);
            SecureLSHIndex idx = (index != null) ? index : new SecureLSHIndex(1, buckets, lshInstance);
            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }


    public void batchInsert(List<String> ids, List<double[]> vectors) {
        if (ids.size() != vectors.size()) {
            throw new IllegalArgumentException("IDs and vectors must be the same size.");
        }

        Map<Integer, List<EncryptedPoint>> byDim = new HashMap<>();

        for (int i = 0; i < vectors.size(); i++) {
            String id = ids.get(i);
            double[] vector = vectors.get(i);
            try {
                int dimension = vector.length;
                DimensionContext ctx = getOrCreateContext(dimension);
                KeyVersion version = keyService.getCurrentVersion();
                SecretKey key = version.getKey();

                int shardId = ctx.getLsh().getBucketId(vector);
                EncryptedPoint encryptedPoint = crypto.encryptToPoint(id, vector, key);
                EncryptedPoint withShard = new EncryptedPoint(
                        encryptedPoint.getId(),
                        shardId,
                        encryptedPoint.getIv(),
                        encryptedPoint.getCiphertext(),
                        version.getVersion(),
                        dimension
                );

                byDim.computeIfAbsent(dimension, k -> new ArrayList<>()).add(withShard);
            } catch (Exception e) {
                logger.error("Failed to encrypt vector id={} during batchInsert", id, e);
            }
        }

        for (Map.Entry<Integer, List<EncryptedPoint>> entry : byDim.entrySet()) {
            DimensionContext ctx = getOrCreateContext(entry.getKey());
            SecureLSHIndex idx = ctx.getIndex();

            long t1 = System.nanoTime();

            for (EncryptedPoint pt : entry.getValue()) {
                indexedPoints.put(pt.getId(), pt);
                idx.addPoint(pt);
                idx.markShardDirty(pt.getShardId());
                pendingMetadata.put(pt.getId(), Map.of(
                        "shardId", String.valueOf(pt.getShardId()),
                        "version", String.valueOf(pt.getVersion())
                ));
            }
            long t2 = System.nanoTime();

            try {
                metadataManager.batchUpdateVectorMetadata(pendingMetadata);
                pendingMetadata.clear();
            } catch (IOException e) {
                logger.error("Failed to batch update metadata", e);
            }

            long t3 = System.nanoTime();

            for (EncryptedPoint pt : entry.getValue()) {
                buffer.add(pt);
                try {
                    metadataManager.saveEncryptedPoint(pt);
                } catch (IOException e) {
                    logger.error("Failed to persist encrypted point {}", pt.getId(), e);
                }
            }

            long t4 = System.nanoTime();

            logger.info("{} points in {} ms (index: {} ms, metaMap: {} ms, save: {} ms)",
                    entry.getValue().size(),
                    TimeUnit.NANOSECONDS.toMillis(t4 - t1),
                    TimeUnit.NANOSECONDS.toMillis(t2 - t1),
                    TimeUnit.NANOSECONDS.toMillis(t3 - t2),
                    TimeUnit.NANOSECONDS.toMillis(t4 - t3)
            );
        }

        buffer.flushAll();
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

        pendingMetadata.put(pt.getId(), Map.of(
                "shardId", String.valueOf(pt.getShardId()),
                "version", String.valueOf(pt.getVersion())
        ));

        try {
            metadataManager.batchUpdateVectorMetadata(pendingMetadata);
            pendingMetadata.clear();
        } catch (IOException e) {
            logger.error("Failed to batch update metadata for point {}", pt.getId(), e);
        }

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
                    vector.length
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