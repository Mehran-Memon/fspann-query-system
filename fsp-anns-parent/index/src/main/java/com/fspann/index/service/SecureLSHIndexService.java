package com.fspann.index.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.core.EvenLSH;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;

import javax.crypto.SecretKey;
import java.util.List;

/**
 * Service layer: encrypts vectors, delegates to core index, handles forward-secure rotation.
 */
public class SecureLSHIndexService implements IndexService {
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;

    public SecureLSHIndexService(SecureLSHIndex index,
                                 CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 EvenLSH lsh) {
        this.index      = index;
        this.crypto     = crypto;
        this.keyService = keyService;
        this.lsh        = lsh;
    }

    /**
     * Core interface method: insert an already-encrypted point.
     */
    @Override
    public void insert(EncryptedPoint pt) {
        index.addPoint(pt);
        index.markShardDirty(pt.getShardId());
    }

    /**
     * Convenience overload: encrypt a raw vector and insert it.
     */
    public void insert(String id, double[] vector) {
        // Rotate keys under forward-security policy
        keyService.rotateIfNeeded();

        // Fetch version and extract raw SecretKey
        KeyVersion version = keyService.getCurrentVersion();
        SecretKey key      = version.getSecretKey();

        // Encrypt the vector
        EncryptedPoint tmp = crypto.encryptToPoint(id, vector, key);

        // Compute correct bucket (shard)
        int bucketId = lsh.getBucketId(vector);

        // Wrap ciphertext with actual shard information
        EncryptedPoint withShard = new EncryptedPoint(
                tmp.getId(),
                bucketId,
                tmp.getIv(),
                tmp.getCiphertext()
        );

        // Delegate to core insert
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
}
