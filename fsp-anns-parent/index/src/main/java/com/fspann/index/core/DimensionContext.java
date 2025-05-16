package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyLifeCycleService;
import com.fspann.key.KeyVersion;
import java.util.List;

/**
 * Associates a context (LSH + index) for re-encryption and dynamic updates.
 */
public class DimensionContext {
    private final EvenLSH lsh;
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    public DimensionContext(int dims,int buckets,
                            CryptoService crypto,
                            KeyLifeCycleService keyService) {
        this.lsh = new EvenLSH(dims, buckets);
        this.index = new SecureLSHIndex(1, buckets);
        this.crypto = crypto;
        this.keyService = keyService;
    }

    public void reEncryptAll(){
        keyService.rotateIfNeeded();
        KeyVersion vOld = keyService.getCurrentVersion();
        KeyVersion vNew = keyService.getCurrentVersion();
        for(int shard: index.getDirtyShards()){
            // re-encryption logic at shard granularity omitted
            index.markShardDirty(shard);
        }
    }
}
