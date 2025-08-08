package com.fspann.index.core;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.crypto.CryptoService;

public class DimensionContext {
    private final EvenLSH lsh;
    private final SecureLSHIndex index;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final int dimension;

    public DimensionContext(SecureLSHIndex index,
                            CryptoService crypto,
                            KeyLifeCycleService keyService,
                            EvenLSH lsh) {
        this.index = index;
        this.crypto = crypto;
        this.keyService = keyService;
        this.lsh = lsh;
        this.dimension = lsh.getDimensions();
    }

    /** No-op in proposal-aligned design (epoch handled at query time). */
    public void reEncryptAll() { /* intentionally empty */ }

    public SecureLSHIndex getIndex() { return index; }
    public EvenLSH getLsh() { return lsh; }
    public int getDimension() { return dimension; }
}
