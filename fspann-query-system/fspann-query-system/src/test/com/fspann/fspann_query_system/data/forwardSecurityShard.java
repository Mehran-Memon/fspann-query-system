package com.fspann.fspann_query_system.data;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecurityShardTest {

    private static final int DIM = 128;
    private static final int NUM_SHARDS = 32;             // must match SecureLSHIndex
    private static final Random RNG = new Random(42);

    private static KeyManager       km;
    private static EvenLSH          lsh;
    private static SecureLSHIndex   index;
    private static List<double[]>   base;                 // dummy base vectors

    @BeforeAll
    static void setUp() {
        km    = new KeyManager(5_000_000);                // rotation threshold
        lsh   = new EvenLSH(DIM, 10);
        base  = randomVectors(5_000, DIM);
        lsh.updateCriticalValues(base);

        index = new SecureLSHIndex(1, km.getCurrentKey(), null);
    }

    @Test
    void forwardSecurityShard() throws Exception {
        double[] v = randomVector(DIM);
        base.add(v);                               // add to corpus for index lookup

        int bucket = lsh.getBucketId(v);
        index.add("id1", v, bucket, false, base);

        km.rotateShard(bucket % NUM_SHARDS);
        index.reEncryptShard(bucket % NUM_SHARDS, km);

        SecretKey oldK = km.getShardKey(bucket % NUM_SHARDS,
                km.getTimeVersion() - 1);

        EncryptedPoint ep = index.findById("id1");
        assertThrows(Exception.class,
                () -> EncryptionUtils.decryptVector(ep.getCiphertext(), oldK));
    }


    /* ------------ helpers ------------ */

    private static double[] randomVector(int d) {
        double[] x = new double[d];
        for (int i = 0; i < d; i++) x[i] = RNG.nextDouble();
        return x;
    }
    private static List<double[]> randomVectors(int n, int d) {
        List<double[]> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) list.add(randomVector(d));
        return list;
    }
}
