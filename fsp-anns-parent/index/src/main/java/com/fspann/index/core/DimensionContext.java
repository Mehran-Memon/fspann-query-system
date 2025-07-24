package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;

import java.util.Collections;
import java.util.List;

public class DimensionContext {
    private final EvenLSH lsh;
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final int dimension;

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
        this.dimension = lsh.getDimensions();
    }

    public void reEncryptAll() {
        KeyVersion current = keyService.getCurrentVersion();
        KeyVersion previous = keyService.getPreviousVersion();
        if (previous == null || current == null) {
            return;
        }
        for (int shard : index.getDirtyShards()) {
            List<Integer> buckets = Collections.singletonList(shard);
            byte[] iv = crypto.generateIV();
            double[] dummyQuery = new double[lsh.getDimensions()];
            byte[] encryptedQuery = crypto.encrypt(dummyQuery, previous.getKey(), iv);
            QueryToken token = new QueryToken(buckets, iv, encryptedQuery, dummyQuery, lsh.getNumBuckets() / 2, index.getNumHashTables(), "epoch_" + previous.getVersion(), lsh.getDimensions(), shard, previous.getVersion());
            List<EncryptedPoint> pts = index.queryEncrypted(token);
            for (EncryptedPoint pt : pts) {
                if (pt.getVersion() <= previous.getVersion()) {
                    EncryptedPoint reEnc = crypto.reEncrypt(pt, current.getKey(), crypto.generateIV());
                    index.removePoint(pt.getId());
                    index.addPoint(reEnc);
                }
            }
            if (!pts.isEmpty()) {
                index.clearDirtyShard(shard);
            }
        }
    }

    public SecureLSHIndex getIndex() {
        return index;
    }

    public EvenLSH getLsh() {
        return lsh;
    }

    public int getDimension() {
        return dimension;
    }
}