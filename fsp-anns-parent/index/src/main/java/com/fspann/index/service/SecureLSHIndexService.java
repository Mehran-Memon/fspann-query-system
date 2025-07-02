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

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final MetadataManager metadataManager;
    private final SecureLSHIndex index; // Optional for testing
    private final EvenLSH lsh;          // Optional for testing

    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final Map<String, EncryptedPoint> indexedPoints = new ConcurrentHashMap<>();
    private final EncryptedPointBuffer buffer;

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 MetadataManager metadataManager) {
        this(crypto, keyService, metadataManager, null, null, createBuffer(metadataManager) );
    }

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 MetadataManager metadataManager,
                                 SecureLSHIndex index,
                                 EvenLSH lsh,
                                 EncryptedPointBuffer buffer) {
        this.crypto = crypto != null ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = keyService;
        this.metadataManager = metadataManager;
        this.index = index;
        this.lsh = lsh;
        this.buffer = buffer;


        try {
            metadataManager.load("metadata.ser");
        } catch (MetadataManager.MetadataException e) {
            logger.warn("Failed to load metadata, starting fresh", e);
        }
    }

    private static EncryptedPointBuffer createBuffer(MetadataManager metadataManager) {
        try {
            return new EncryptedPointBuffer("metadata/points", metadataManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize EncryptedPointBuffer", e);
        }
    }

    private DimensionContext getOrCreateContext(int dimension) {
        return dimensionContexts.computeIfAbsent(dimension, dim -> {
            int buckets = 32;
            EvenLSH lshInstance = (lsh != null) ? lsh : new EvenLSH(dim, buckets);
            SecureLSHIndex idx = (index != null) ? index : new SecureLSHIndex(1, buckets, lshInstance);
            return new DimensionContext(idx, crypto, keyService, lshInstance);
        });
    }

    public void batchInsert(List<String> ids, List<double[]> vectors) {
        if (ids.size() != vectors.size()) {
            throw new IllegalArgumentException("IDs and vectors must be the same size.");
        }

        metadataManager.setDeferSave(true); // Disable metadata.ser writes temporarily

        Map<Integer, List<EncryptedPoint>> byDim = new HashMap<>();

        for (int i = 0; i < vectors.size(); i++) {
            String id = ids.get(i);
            double[] vector = vectors.get(i);
            try {
                int dimension = vector.length;
                DimensionContext ctx = getOrCreateContext(dimension);
                keyService.rotateIfNeeded();
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
            }
            long t2 = System.nanoTime();

            for (EncryptedPoint pt : entry.getValue()) {
                metadataManager.putVectorMetadata(pt.getId(), String.valueOf(pt.getShardId()), String.valueOf(pt.getVersion()));
            }
            long t3 = System.nanoTime();

            for (EncryptedPoint pt : entry.getValue()) {
                buffer.add(pt);
            }
            long t4 = System.nanoTime();

            logger.info("{} ms (index: {} ms, metaMap: {} ms, save: {} ms)",
                    entry.getValue().size(),
                    TimeUnit.NANOSECONDS.toMillis(t2 - t1),
                    TimeUnit.NANOSECONDS.toMillis(t3 - t2),
                    TimeUnit.NANOSECONDS.toMillis(t4 - t3)
            );
        }

        try {
            metadataManager.setDeferSave(false); // Enable metadata.ser saving again
            metadataManager.save("metadata.ser"); // Write once at the end
//            logger.info("Flushed metadata.ser after batch.");
        } catch (MetadataManager.MetadataException e) {
            logger.error("Failed to save metadata after batchInsert", e);
        }
        buffer.flushAll();
    }

    @Override
    public void insert(EncryptedPoint pt) {
        int dimension = pt.getVectorLength();
        DimensionContext ctx = getOrCreateContext(dimension);
        indexedPoints.put(pt.getId(), pt);
        SecureLSHIndex idx = ctx.getIndex();
        idx.addPoint(pt);
        idx.markShardDirty(pt.getShardId());
        metadataManager.putVectorMetadata(pt.getId(), String.valueOf(pt.getShardId()), String.valueOf(pt.getVersion()));
        buffer.add(pt);       //Persist encrypted point to disk
        try {
            metadataManager.save("metadata.ser");
        } catch (MetadataManager.MetadataException e) {
            logger.error("Failed to save metadata", e);
        }
    }

    @Override
    public void insert(String id, double[] vector) {
        int dimension = vector.length;
        DimensionContext ctx = getOrCreateContext(dimension);

        keyService.rotateIfNeeded();
        KeyVersion version = keyService.getCurrentVersion();
        SecretKey key = version.getKey();

        try {
            int shardId = ctx.getLsh().getBucketId(vector);
            EncryptedPoint encryptedPoint = crypto.encryptToPoint(id, vector, key);
            EncryptedPoint withShard = new EncryptedPoint(
                    encryptedPoint.getId(),
                    shardId,
                    encryptedPoint.getIv(),
                    encryptedPoint.getCiphertext(),
                    version.getVersion(),
                    vector.length
            );
            insert(withShard);
        } catch (Exception e) {
            logger.error("Failed to encrypt vector id={}", id, e);
            throw new RuntimeException("Encryption failed for id=" + id, e);
        }
    }

    @Override
    public void delete(String id) {
        EncryptedPoint pt = indexedPoints.remove(id);
        if (pt != null) {
            int dimension = pt.getVectorLength();
            DimensionContext ctx = dimensionContexts.get(dimension);
            if (ctx != null) {
                ctx.getIndex().removePoint(id);
            }
        }
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
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

    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        EncryptedPoint cached = indexedPoints.get(id);
        if (cached != null) return cached;
        try {
            return metadataManager.loadEncryptedPoint(id);
        } catch (Exception e) {
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

    public void shutdown() {
        buffer.shutdown(); // now a no-op
    }


}
