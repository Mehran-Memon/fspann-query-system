package com.fspann.query.core;

import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import org.junit.jupiter.api.*;
import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryTokenFactoryPureTest {

    private CryptoService crypto;
    private KeyLifeCycleService keys;
    private QueryTokenFactory factory;

    @BeforeEach
    void init() {
        crypto = mock(CryptoService.class);
        keys   = mock(KeyLifeCycleService.class);

        // Option-C parameters: ell=2, m=3, seed=13
        factory = new QueryTokenFactory(
                crypto,
                keys,
                null,      // no LSH
                0,         // numTables
                0,         // probeRange unused
                2,         // ell
                3,         // m
                13L
        );
    }

    @Test
    void createToken_generatesCodes_onlyOptionC() {
        double[] q = {1.0, 2.0};
        var tok = factory.create(q, 5);

        assertEquals(5, tok.getTopK());
        assertEquals(2, tok.getDimension());
        assertNull(tok.getTableBuckets()); // Option-C never uses buckets

        BitSet[] codes = tok.getCodes();
        assertNotNull(codes);
        assertEquals(2, codes.length);  // ell=2
        assertEquals(3, codes[0].length()); // projection count m=3
    }

    @Test
    void nullVector_throwsIAE() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(null, 5)
        );
        assertTrue(ex.getMessage().toLowerCase().contains("vector"));
    }

    @Test
    void zeroLengthVector_throwsIAE() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(new double[]{}, 5)
        );
        assertTrue(ex.getMessage().toLowerCase().contains("vector"));
    }

    @Test
    void negativeTopK_throwsIAE() {
        double[] q = {1, 2};
        assertThrows(IllegalArgumentException.class,
                () -> factory.create(q, -1));
    }

    @Test
    void createToken_isDeterministic_givenSameSeed() {
        double[] q = {0.1, 0.2};
        var a = factory.create(q, 3);
        var b = factory.create(q, 3);

        // Codes must match exactly.
        for (int i = 0; i < a.getCodes().length; i++) {
            assertEquals(a.getCodes()[i], b.getCodes()[i]);
        }
    }
}
