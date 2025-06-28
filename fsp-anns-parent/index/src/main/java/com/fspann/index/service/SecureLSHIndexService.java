package com.fspann.index.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.core.EvenLSH;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.MetadataManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final MetadataManager metadataManager;
    private final SecureLSHIndex index; // For testing
    private final EvenLSH lsh; // For testing

    private final Map<Integer, DimensionContext> dimensionContexts = new ConcurrentHashMap<>();
    private final Map<String, EncryptedPoint> indexedPoints = new ConcurrentHashMap<>();

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 MetadataManager metadataManager) {
        this(crypto, keyService, metadataManager, null, null);
    }

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 MetadataManager metadataManager,
                                 SecureLSHIndex index,
                                 EvenLSH lsh) {
        this.crypto = crypto != null ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = keyService;
        this.metadataManager = metadataManager;
        this.index = index;
        this.lsh = lsh;

        try {
            metadataManager.load("metadata.ser");
        } catch (MetadataManager.MetadataException e) {
            logger.warn("Failed to load metadata, starting fresh", e);
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

    @Override
    public void insert(EncryptedPoint pt) {
        int dimension = pt.getVectorLength();
        DimensionContext ctx = getOrCreateContext(dimension);
        indexedPoints.put(pt.getId(), pt);
        SecureLSHIndex idx = ctx.getIndex();
        idx.addPoint(pt);
        idx.markShardDirty(pt.getShardId());
        metadataManager.putVectorMetadata(pt.getId(), String.valueOf(pt.getShardId()), String.valueOf(pt.getVersion()));
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

    public EncryptedPoint getEncryptedPoint(String id) {
        return indexedPoints.get(id);
    }
}
