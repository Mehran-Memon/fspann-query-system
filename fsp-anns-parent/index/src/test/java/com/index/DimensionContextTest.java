package com.index;

import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.index.core.DimensionContext;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DimensionContextTest {

    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private SecureLSHIndex index;
    private EvenLSH lsh;

    @BeforeEach
    void setUp() {
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        lsh = mock(EvenLSH.class);
        when(lsh.getDimensions()).thenReturn(128);
        when(lsh.getNumBuckets()).thenReturn(64);
        index = mock(SecureLSHIndex.class);
    }

    @Test
    void testContextWiresDependencies() {
        DimensionContext ctx = new DimensionContext(index, crypto, keyService, lsh);
        assertSame(index, ctx.getIndex());
        assertSame(lsh, ctx.getLsh());
        assertEquals(128, ctx.getDimension());
    }
}
