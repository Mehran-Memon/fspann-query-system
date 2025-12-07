package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class QueryEndToEndPaperModeTest {

    private SecureLSHIndexService indexService;
    private QueryServiceImpl qs;

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private RocksDBMetadataManager meta;
    private EncryptedPointBuffer buffer;
    private PartitionedIndexService paper;

    private QueryTokenFactory tokenFactory;

    private final Map<String,double[]> plain = new ConcurrentHashMap<>();

    private static final SecretKey KEY = new SecretKeySpec(new byte[32],"AES");
    private static final KeyVersion KV1 = new KeyVersion(1, KEY);

    private final double[][] points = {
            {0,0},
            {1,0},
            {0,1},
            {1,1},
            {5,5},
            {5.1,5},
            {10,10},
            {10.1,10}
    };

    @BeforeEach
    void setup() throws Exception {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);
        meta   = mock(RocksDBMetadataManager.class);
        buffer = mock(EncryptedPointBuffer.class);

        when(keys.getCurrentVersion()).thenReturn(KV1);
        when(keys.getVersion(anyInt())).thenReturn(KV1);

        when(meta.getPointsBaseDir()).thenReturn("tmpdir");

        // Encrypt = just store plaintext for recovery
        when(crypto.encrypt(anyString(), any(double[].class)))
                .thenAnswer(inv -> {
                    String id = inv.getArgument(0,String.class);
                    double[] v = inv.getArgument(1,double[].class);
                    plain.put(id, v.clone());
                    return new EncryptedPoint(id,0,new byte[12],new byte[32],1,v.length,List.of());
                });

        when(crypto.encryptToPoint(eq("query"), any(), any()))
                .thenAnswer(inv -> {
                    double[] v = inv.getArgument(1,double[].class);
                    return new EncryptedPoint("query",0,new byte[12],new byte[32],1,v.length,List.of());
                });

        when(crypto.decryptFromPoint(any(), any()))
                .thenAnswer(inv -> {
                    EncryptedPoint ep = inv.getArgument(0);
                    return plain.get(ep.getId());
                });

        // PAPER engine
        paper = new PartitionedIndexService(
                8,4,3,13L,
                1,            // build threshold
                -1            // unlimited candidates
        );

        indexService = new SecureLSHIndexService(crypto, keys, meta, paper, buffer);

        // Option-C TokenFactory (no LSH)
        tokenFactory = new QueryTokenFactory(
                crypto,
                keys,
                null,       // NO LSH
                0,          // numTables
                0,          // probeRange
                4,          // divisions
                3,          // m
                13L         // seedBase
        );

        qs = new QueryServiceImpl(indexService, crypto, keys, tokenFactory);

        for (int i=0; i<points.length; i++) {
            indexService.insert(String.valueOf(i), points[i]);
        }

        forceBuild(paper);
    }

    private void forceBuild(PartitionedIndexService p) throws Exception {
        Field f = PartitionedIndexService.class.getDeclaredField("dims");
        f.setAccessible(true);
        Map<Integer,Object> dims = (Map<Integer,Object>) f.get(p);
        Method m = PartitionedIndexService.class.getDeclaredMethod("buildAllDivisions",
                dims.values().iterator().next().getClass());
        m.setAccessible(true);
        for (Object d : dims.values()) m.invoke(p,d);
    }

    private double d2(double[] a,double[]b){
        double s=0;
        for(int i=0;i<a.length;i++){
            double d=a[i]-b[i]; s+=d*d;
        }
        return s;
    }

    private String nearest(double[] q,List<QueryResult> res){
        double best=Double.MAX_VALUE; String bestId=null;
        for (QueryResult r : res) {
            double[] v = plain.get(r.getId());
            double d   = d2(q,v);
            if (d < best) { best = d; bestId = r.getId(); }
        }
        return bestId;
    }

    @Test
    void nearest_from_dense_cluster_is_returned_first() {
        double[] q = {5.05,5.0};

        when(crypto.decryptQuery(any(),any(),any())).thenReturn(q.clone());

        QueryToken tok = tokenFactory.create(q, 3);

        List<QueryResult> out = qs.search(tok);

        assertFalse(out.isEmpty());
        assertEquals(nearest(q,out), out.get(0).getId());
    }

    @Test
    void far_query_returns_far_cluster() {
        double[] q = {10.05,10};

        when(crypto.decryptQuery(any(),any(),any())).thenReturn(q.clone());

        QueryToken tok = tokenFactory.create(q, 2);

        List<QueryResult> out = qs.search(tok);

        assertFalse(out.isEmpty());
        assertEquals(nearest(q,out), out.get(0).getId());
    }

    @Test
    void metrics_are_populated() {
        double[] q = {1,1};

        when(crypto.decryptQuery(any(),any(),any())).thenReturn(q.clone());

        QueryToken tok = tokenFactory.create(q, 5);

        qs.search(tok);

        assertTrue(qs.getLastCandTotal() >= qs.getLastReturned());
        assertTrue(qs.getLastCandDecrypted() >= qs.getLastReturned());
        assertTrue(qs.getLastQueryDurationNs() > 0);
    }
}
