package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;

import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * QueryServiceImplMinimalTest - UPDATED for Dual Engine Support
 *
 * Tests error handling and edge cases for PARTITIONED mode.
 * Uses mock IndexService which falls back to safeLookup() behavior.
 */
public class QueryServiceImplMinimalTest {

    CryptoService crypto;
    KeyLifeCycleService keys;
    IndexService index;
    EncryptedPointBuffer buffer;
    QueryServiceImpl svc;
    SecretKey k;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys = mock(KeyLifeCycleService.class);
        index = mock(IndexService.class);
        buffer = mock(EncryptedPointBuffer.class);

        // Create a mock for SystemConfig
        SystemConfig cfg = mock(SystemConfig.class);

        // global non-null AES key
        k = new SecretKeySpec(new byte[16], "AES");

        when(keys.getCurrentVersion()).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(1)).thenReturn(new KeyVersion(1, k));
        when(keys.getVersion(anyInt())).thenReturn(new KeyVersion(1, k));

        // Mock buffer
        when(index.getPointBuffer()).thenReturn(buffer);

        // Initialize service with mocked IndexService (not SecureLSHIndexService)
        // This makes it use safeLookup() fallback behavior
        svc = new QueryServiceImpl(index, crypto, keys, null, cfg);
    }

    // ========================================================================
    // PARTITIONED MODE TESTS (Default behavior - no SecureLSHIndexService)
    // ========================================================================

    @Test
    void nullDecryption_skipsPoint() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint bad = new EncryptedPoint("x", 0, iv, ct, 1, 2, List.of());
        when(index.lookup(any())).thenReturn(List.of(bad));

        when(crypto.decryptFromPoint(bad, k)).thenReturn(null);

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandDecrypted());
    }

    @Test
    void malformedVectors_doNotCrash() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p = new EncryptedPoint("m", 0, iv, ct, 1, 2, List.of());
        when(index.lookup(any())).thenReturn(List.of(p));

        when(crypto.decryptFromPoint(p, k))
                .thenReturn(new double[]{Double.NaN, 0});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandDecrypted());
    }

    @Test
    void infinityValues_skipped() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p = new EncryptedPoint("inf", 0, iv, ct, 1, 2, List.of());
        when(index.lookup(any())).thenReturn(List.of(p));

        when(crypto.decryptFromPoint(p, k))
                .thenReturn(new double[]{Double.POSITIVE_INFINITY, 0});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "ctx", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        assertEquals(0, svc.getLastCandDecrypted());
    }

    @Test
    void nullQueryDecryption_returnsEmpty() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k)).thenReturn(null);

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
        verify(index, never()).lookup(any());
    }

    @Test
    void emptyQueryVector_returnsEmpty() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k)).thenReturn(new double[]{});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertTrue(out.isEmpty());
    }

    @Test
    void nullToken_returnsEmpty() {
        List<QueryResult> out = svc.search(null);

        assertTrue(out.isEmpty());
        verify(index, never()).lookup(any());
    }

    @Test
    void duplicateCandidates_deduplicated() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p = new EncryptedPoint("dup", 0, iv, ct, 1, 2, List.of());

        // Same point returned twice
        when(index.lookup(any())).thenReturn(List.of(p, p));

        when(crypto.decryptFromPoint(p, k))
                .thenReturn(new double[]{1, 1});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        // Should only decrypt once (deduplicated)
        assertEquals(1, out.size());
        assertEquals("dup", out.get(0).getId());
        assertEquals(1, svc.getLastCandDecrypted());
    }

    @Test
    void metricsCollection_tracksState() {
        byte[] iv = new byte[12];
        byte[] ct = new byte[16];

        when(crypto.decryptQuery(ct, iv, k))
                .thenReturn(new double[]{0, 0});

        EncryptedPoint p1 = new EncryptedPoint("p1", 0, iv, ct, 1, 2, List.of());
        EncryptedPoint p2 = new EncryptedPoint("p2", 0, iv, ct, 1, 2, List.of());

        when(index.lookup(any())).thenReturn(List.of(p1, p2));

        when(crypto.decryptFromPoint(p1, k))
                .thenReturn(new double[]{1, 1});
        when(crypto.decryptFromPoint(p2, k))
                .thenReturn(new double[]{0.5, 0.5});

        QueryToken t = new QueryToken(List.of(), null, iv, ct, 5, 1, "c", 2, 1);

        List<QueryResult> out = svc.search(t);

        assertEquals(2, svc.getLastCandTotal());
        assertEquals(2, svc.getLastCandDecrypted());
        assertEquals(2, svc.getLastReturned());
        assertNotNull(svc.getLastCandidateIds());
        assertTrue(svc.getLastCandidateIds().containsAll(List.of("p1", "p2")));
    }
}