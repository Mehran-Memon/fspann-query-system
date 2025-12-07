package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class QueryCandidateTrackingTest {

    private static class MockCrypto implements CryptoService {
        @Override public EncryptedPoint encryptToPoint(String id,double[] v,SecretKey k){
            return new EncryptedPoint(id,0,new byte[12],new byte[]{1},5,v.length,List.of());
        }
        @Override public double[] decryptFromPoint(EncryptedPoint p,SecretKey k){
            return (p.getVersion()==5)? new double[]{1} : null;
        }
        @Override public byte[] encrypt(double[] v, SecretKey k, byte[] iv){ return new byte[]{1}; }
        @Override public double[] decryptQuery(byte[] q, byte[] iv, SecretKey k){ return new double[]{1}; }
        @Override public EncryptedPoint reEncrypt(EncryptedPoint p, SecretKey k, byte[] iv){
            return new EncryptedPoint(p.getId(),p.getShardId(),iv,new byte[]{1},5,p.getVectorLength(),p.getBuckets());
        }
        @Override public byte[] generateIV(){ return new byte[12]; }
        @Override public KeyLifeCycleService getKeyService(){ return null; }
        @Override public EncryptedPoint encrypt(String id,double[] v){
            return encryptToPoint(id,v,new SecretKeySpec(new byte[16],"AES"));
        }
    }

    private static class MockKeys implements KeyLifeCycleService {
        SecretKey k = new SecretKeySpec(new byte[16],"AES");
        KeyVersion v5 = new KeyVersion(5,k);
        public void rotateIfNeeded() {}
        public void incrementOperation() {}
        public KeyVersion getCurrentVersion(){ return v5; }
        public KeyVersion getPreviousVersion(){ return v5; }
        public KeyVersion getVersion(int ver){ return v5; }
        public void reEncryptAll() {}
    }

    private static class MockIndex implements IndexService {
        public List<EncryptedPoint> lookup(QueryToken t){
            return List.of(
                    new EncryptedPoint("A",0,new byte[12],new byte[]{1},3,1,List.of()),
                    new EncryptedPoint("B",0,new byte[12],new byte[]{1},5,1,List.of())
            );
        }
        public void insert(EncryptedPoint p){}
        public void insert(String id,double[]v){}
        public void delete(String id){}
        public void markDirty(int shard){}
        public int getIndexedVectorCount(){ return 2; }
        public Set<Integer> getRegisteredDimensions(){ return Set.of(1); }
        public int getVectorCountForDimension(int d){ return 2; }
        public EncryptedPoint getEncryptedPoint(String id){ return null; }
        public void updateCachedPoint(EncryptedPoint p){}
        public EncryptedPointBuffer getPointBuffer(){ return null; }
        public int getShardIdForVector(double[] v){ return 0; }
    }

    @Test
    void testIdealSelectiveTouchRules() {

        MockCrypto crypto = new MockCrypto();
        MockKeys keys = new MockKeys();

        QueryTokenFactory tf = new QueryTokenFactory(
                crypto, keys,
                null,      // LSH
                0,0,       // numTables, probeRange
                1,1,       // divisions,m
                13L
        );

        QueryServiceImpl svc = new QueryServiceImpl(
                new MockIndex(), crypto, keys, tf
        );

        QueryToken tok = tf.create(new double[]{1.0}, 1);
        List<QueryResult> out = svc.search(tok);

        assertNotNull(out);
        assertEquals(Set.of("A","B"), new HashSet<>(svc.getLastCandidateIds()));
        assertEquals(2, svc.getLastTouchedCumulativeUnique());
        assertEquals(1, svc.getLastCandDecrypted());
        assertEquals(2, svc.getLastCandTotal());
        assertEquals(1, svc.getLastReturned());
    }
}
