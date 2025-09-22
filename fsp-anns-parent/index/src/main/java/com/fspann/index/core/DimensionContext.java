package com.fspann.index.core;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.crypto.CryptoService;

/**
 * DimensionContext
 * Single source of truth (per vector dimension) for:
 *  - The EvenLSH instance used by the legacy multiprobe path.
 *  - The in-memory SecureLSHIndex (legacy path).
 *  - Crypto + key lifecycle hooks shared by the service.
 *
 * Paper/partitioned mode does NOT use this context; it owns its own structures.
 */
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
        if (index == null) throw new IllegalArgumentException("index is null");
        if (crypto == null) throw new IllegalArgumentException("crypto is null");
        if (keyService == null) throw new IllegalArgumentException("keyService is null");
        if (lsh == null) throw new IllegalArgumentException("lsh is null");

        this.index = index;
        this.crypto = crypto;
        this.keyService = keyService;
        this.lsh = lsh;
        this.dimension = lsh.getDimensions();
    }

    /** No-op for legacy path; paper path handles epoch at query time. */
    public void reEncryptAll() { /* intentionally empty */ }

    public SecureLSHIndex getIndex() { return index; }
    public EvenLSH getLsh() { return lsh; }
    public CryptoService getCrypto() { return crypto; }
    public KeyLifeCycleService getKeyService() { return keyService; }
    public int getDimension() { return dimension; }
}
