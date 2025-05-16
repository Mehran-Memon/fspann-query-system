package com.fspann.index.service;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.key.KeyLifeCycleService;
import com.fspann.key.KeyVersion;

import javax.crypto.SecretKey;
import java.util.List;

/**
 * Service layer: encrypts vectors, delegates to core index, handles rotation.
 */
public class SecureLSHIndexService implements IndexService {
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    public SecureLSHIndexService(SecureLSHIndex index,
                                 CryptoService crypto,
                                 KeyLifeCycleService keyService) {
        this.index      = index;
        this.crypto     = crypto;
        this.keyService = keyService;
    }

    /**
     * Encrypts a raw vector, rotates keys if needed, and adds the point.
     */
    @Override
    public void insert(String id, double[] vector) {
        KeyVersion ver = keyService.getCurrentVersion();
        SecretKey key   = ver.getSecretKey();
        EncryptedPoint pt = crypto.encryptToPoint(id, vec, key);
        // now wrap it with the real shard:
        EncryptedPoint withShard = new EncryptedPoint(
                pt.getId(),
                bucketId,
                pt.getIv(),
                pt.getCiphertext()
        );
        index.addPoint(withShard);
        index.markShardDirty(pt.getShardId());
    }

    @Override
    public void delete(String id) {
        index.removePoint(id);
    }

    @Override
    public List<EncryptedPoint> lookup(QueryToken token) {
        return index.queryEncrypted(token);
    }

    @Override
    public void markDirty(int shardId) {
        index.markShardDirty(shardId);
    }
}
