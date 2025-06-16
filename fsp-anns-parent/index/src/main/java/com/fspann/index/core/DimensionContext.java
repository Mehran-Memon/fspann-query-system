package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;

import java.util.Collections;
import java.util.List;

/**
 * Associates a context (LSH + index) for re-encryption and dynamic updates.
 */
public class DimensionContext {
    private final EvenLSH lsh;
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    /** Production constructor */
    public DimensionContext(int dims, int buckets,
                            CryptoService crypto,
                            KeyLifeCycleService keyService) {
        this(new SecureLSHIndex(1, buckets, new EvenLSH(dims, buckets)),
                crypto, keyService,
                new EvenLSH(dims, buckets));
    }

    public DimensionContext(
            SecureLSHIndex index,
            CryptoService crypto,
            KeyLifeCycleService keyService,
            EvenLSH lsh
    ) {
        this.index = index;
        this.crypto = crypto;
        this.keyService = keyService;
        this.lsh = lsh;
    }

    public void reEncryptAll() {
        // 1) Rotate keys so that getPreviousVersion() is the “old” one
        keyService.rotateIfNeeded();
        KeyVersion previous = keyService.getPreviousVersion();
        KeyVersion current = keyService.getCurrentVersion();

        // 2) For each dirty shard, build a token that grabs every point in that shard:
        for (int shard : index.getDirtyShards()) {
            List<Integer> buckets = Collections.singletonList(shard);
            // Generate new IV and encrypt a dummy query with the previous key
            byte[] iv = crypto.generateIV();
            double[] dummyQuery = new double[lsh.getDimensions()];
            byte[] encryptedQuery = crypto.encrypt(dummyQuery, previous.getKey(), iv);

            // topK = half the number of buckets (10/2 == 5 in your test)
            int topK = lsh.getNumBuckets() / 2;
            int numTables = 1;
            // context = “epoch_0” when previous.getVersion() == 1
            String context = "epoch_" + (previous.getVersion() - 1);

            QueryToken token = new QueryToken(
                    buckets,
                    iv,
                    encryptedQuery,
                    dummyQuery,
                    topK,
                    numTables,
                    context
            );

            // 3) Pull and re-encrypt
            List<EncryptedPoint> pts = index.queryEncrypted(token);
            for (EncryptedPoint pt : pts) {
                EncryptedPoint reEnc = crypto.reEncrypt(pt, current.getKey());
                index.removePoint(pt.getId());
                index.addPoint(reEnc);
            }

            // 4) clear the flag
            index.clearDirtyShard(shard);
        }
    }
}