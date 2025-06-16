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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service layer: encrypts vectors, delegates to core index, handles forward-secure rotation.
 */
public class SecureLSHIndexService implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndexService.class);
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    private final Map<String, EncryptedPoint> indexedPoints = new HashMap<>();
    private final MetadataManager metadataManager;
    private final DimensionContext dimensionContext;

    public SecureLSHIndexService(SecureLSHIndex index,
                                 CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 EvenLSH lsh,
                                 MetadataManager metadataManager,
                                 int dims,
                                 int buckets) {
        this.index = index;
        this.crypto = crypto != null ? crypto : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        this.keyService = keyService;
        this.lsh = lsh;
        this.metadataManager = metadataManager;
        this.dimensionContext = new DimensionContext(dims, buckets, this.crypto, keyService);
        try {
            metadataManager.load("metadata.ser");
        } catch (MetadataManager.MetadataException e) {
            logger.warn("Failed to load metadata, starting fresh", e);
        }
    }

    /**
     * Core interface method: insert an already-encrypted point.
     */
    @Override
    public void insert(EncryptedPoint pt) {
        indexedPoints.put(pt.getId(), pt);
        index.addPoint(pt);
        index.markShardDirty(pt.getShardId());
        metadataManager.putVectorMetadata(
                pt.getId(),
                String.valueOf(pt.getShardId()),
                String.valueOf(keyService.getCurrentVersion().getVersion())
        );
        try {
            metadataManager.save("metadata.ser");
        } catch (MetadataManager.MetadataException e) {
            logger.error("Failed to save metadata", e);
        }
    }

    /**
     * Convenience overload: encrypt a raw vector and insert it.
     */
    public void insert(String id, double[] vector) {
        logger.debug("Inserting vector into index with id={} and vector={}", id, Arrays.toString(vector));

        // Rotate keys under forward-security policy
        keyService.rotateIfNeeded();

        // Fetch current version and SecretKey
        KeyVersion version = keyService.getCurrentVersion();
        SecretKey key = version.getKey(); // Changed from getSecretKey() to getKey()

        // Encrypt the vector
        EncryptedPoint encryptedPoint;
        try {
            encryptedPoint = crypto.encryptToPoint(id, vector, key);
        } catch (Exception e) {
            logger.error("Failed to encrypt vector id={}", id, e);
            throw new RuntimeException("Encryption failed for id=" + id, e);
        }

        // Determine shard: honor the shardId from the EncryptedPoint
        int shardId = encryptedPoint.getShardId();

        // Wrap with version metadata
        EncryptedPoint withShard = new EncryptedPoint(
                encryptedPoint.getId(),
                shardId,
                encryptedPoint.getIv(),
                encryptedPoint.getCiphertext(),
                version.getVersion()
        );

        // Delegate to core insertion
        insert(withShard);
    }

    /**
     * Remove a point by its ID.
     */
    @Override
    public void delete(String id) {
        index.removePoint(id);
    }

    /**
     * Query encrypted points and return up to top-K from each table.
     */
    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        return index.queryEncrypted(token);
    }

    /**
     * Mark a shard as dirty (pending re-encryption).
     */
    @Override
    public void markDirty(int shardId) {
        index.markShardDirty(shardId);
    }

    public int getIndexedVectorCount() {
        return indexedPoints.size();
    }
}