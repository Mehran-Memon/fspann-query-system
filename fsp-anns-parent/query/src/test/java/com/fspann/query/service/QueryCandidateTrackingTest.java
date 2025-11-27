package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FINAL, CLEAN, COMPILE-READY TEST
 * -------------------------------
 * Enforces IDEAL selective-touch rules:
 *
 *   Touch a point if:
 *      1) point.version != currentVersion  (stale → needs re-encryption)
 *      2) point.version == currentVersion AND decryptFromPoint succeeds
 *
 *   Do not touch:
 *      - fresh points that fail decryption
 *      - anything not returned by lookup
 */
public class QueryCandidateTrackingTest {

    // ----------------------------------------------------------------------
    // Mock CryptoService – full interface implemented
    // ----------------------------------------------------------------------
    private static class MockCrypto implements CryptoService {

        @Override
        public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
            return new EncryptedPoint(
                    id,
                    /* shardId */ 0,
                    /* iv */ new byte[12],
                    /* ciphertext */ new byte[]{1},
                    /* version */ 5,
                    /* vectorLength */ vector.length,
                    /* buckets */ List.of()
            );
        }

        @Override
        public double[] decryptFromPoint(EncryptedPoint pt, SecretKey key) {
            // Only decrypt version=5 points
            if (pt.getVersion() == 5) {
                return new double[]{1.0};
            }
            return null; // stale or incompatible
        }

        @Override
        public byte[] encrypt(double[] vector, SecretKey key, byte[] iv) {
            return new byte[]{1};
        }

        @Override
        public double[] decryptQuery(byte[] encryptedQuery, byte[] iv, SecretKey key) {
            return new double[]{1.0}; // trivial 1D vector
        }

        @Override
        public EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey, byte[] newIv) {
            return new EncryptedPoint(
                    pt.getId(),
                    pt.getShardId(),
                    newIv,
                    new byte[]{1},
                    5,
                    pt.getVectorLength(),
                    pt.getBuckets()
            );
        }

        @Override
        public byte[] generateIV() {
            return new byte[12];
        }

        @Override
        public KeyLifeCycleService getKeyService() {
            return null; // not needed
        }

        @Override
        public EncryptedPoint encrypt(String id, double[] vector) {
            return encryptToPoint(id, vector, generateDummyKey());
        }

        private SecretKey generateDummyKey() {
            return new SecretKeySpec(new byte[]{
                    1,2,3,4,5,6,7,8,
                    9,1,2,3,4,5,6,7
            }, "AES");
        }
    }

    // ----------------------------------------------------------------------
    // Mock Key Service – matches EXACT interface provided by user
    // ----------------------------------------------------------------------
    private static class MockKeyService implements KeyLifeCycleService {

        private final SecretKey key =
                new SecretKeySpec(new byte[]{
                        9,9,9,9,9,9,9,9,
                        9,9,9,9,9,9,9,9
                }, "AES");

        private final KeyVersion v5 = new KeyVersion(5, key);

        @Override
        public void rotateIfNeeded() { }

        @Override
        public void incrementOperation() { }

        @Override
        public KeyVersion getCurrentVersion() {
            return v5;
        }

        @Override
        public KeyVersion getPreviousVersion() {
            return v5; // trivial mock
        }

        @Override
        public KeyVersion getVersion(int version) {
            if (version != 5) {
                throw new IllegalArgumentException("Mock only supports version=5");
            }
            return v5;
        }

        @Override
        public void reEncryptAll() { }
    }

    // ----------------------------------------------------------------------
    // Mock Index – returns one stale and one fresh point
    // ----------------------------------------------------------------------
    private static class MockIndex implements IndexService {

        @Override
        public List<EncryptedPoint> lookup(QueryToken t) {

            EncryptedPoint stale = new EncryptedPoint(
                    "A",
                    /* shard */ 0,
                    /* iv */ new byte[12],
                    /* ciphertext */ new byte[]{1},
                    /* version */ 3,
                    /* vectorLength */ 1,
                    /* buckets */ List.of()
            );

            EncryptedPoint fresh = new EncryptedPoint(
                    "B",
                    /* shard */ 0,
                    /* iv */ new byte[12],
                    /* ciphertext */ new byte[]{1},
                    /* version */ 5,
                    /* vectorLength */ 1,
                    /* buckets */ List.of()
            );

            return List.of(stale, fresh);
        }

        @Override public void insert(EncryptedPoint pt) { }
        @Override public void insert(String id, double[] vector) { }
        @Override public void delete(String id) { }
        @Override public void markDirty(int shardId) { }
        @Override public int getIndexedVectorCount() { return 2; }
        @Override public Set<Integer> getRegisteredDimensions() { return Set.of(1); }
        @Override public int getVectorCountForDimension(int dimension) { return 2; }
        @Override public EncryptedPoint getEncryptedPoint(String id) { return null; }
        @Override public void updateCachedPoint(EncryptedPoint pt) { }
        @Override public EncryptedPointBuffer getPointBuffer() { return null; }
        @Override public int getShardIdForVector(double[] vector) { return 0; }
    }

    // ----------------------------------------------------------------------
    // Minimal Token Factory – uses latest QueryToken constructor
    // ----------------------------------------------------------------------
    private static class MockTokenFactory extends QueryTokenFactory {
        public MockTokenFactory(CryptoService crypto, KeyLifeCycleService keys) {
            super(
                    crypto,        // CryptoService
                    keys,          // KeyLifeCycleService
                    null,          // EvenLSH (null → pure paper mode)
                    0,             // expansionRange
                    0              // numTables
            );
        }

        @Override
        public QueryToken create(double[] vec, int topK) {
            return new QueryToken(
                    List.of(),                 // tableBuckets
                    new BitSet[]{ new BitSet() }, // codes
                    new byte[12],              // iv (valid AES-GCM length)
                    new byte[]{1},             // encryptedQuery
                    topK,
                    0,                         // numTables
                    "epoch_5_dim_1",
                    1,                         // dimension
                    5                          // version
            );
        }
    }


    // =====================================================================
    //                      FINAL ACTUAL TEST
    // =====================================================================
    @Test
    public void testCandidateTrackingIdealBehavior() {

        MockCrypto crypto = new MockCrypto();
        MockKeyService keys = new MockKeyService();
        MockTokenFactory tf = new MockTokenFactory(crypto, keys);

        QueryServiceImpl svc = new QueryServiceImpl(
                new MockIndex(),
                crypto,
                keys,
                tf
        );

        QueryToken tok = tf.create(new double[]{1.0}, 1);

        List<QueryResult> out = svc.search(tok);
        assertNotNull(out);

        // candidate set = A,B
        assertEquals(Set.of("A", "B"),
                new HashSet<>(svc.getLastCandidateIds()));

        // ideal selective-touch: stale (A) OR decryptable (B) → 2
        assertEquals(2, svc.getLastTouchedCumulativeUnique());

        // Only B decryptable
        assertEquals(1, svc.getLastCandDecrypted());

        assertEquals(2, svc.getLastCandTotal());
        assertEquals(1, svc.getLastReturned());
    }

}
