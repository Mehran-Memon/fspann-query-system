package com.fspann.common;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Objects;

/**
 * QueryToken — MSANNP ONLY (Peng et al.)
 * =====================================
 *
 * This token carries:
 *  - Encrypted query payload (IV + ciphertext)
 *  - Precomputed MSANNP bit codes per table/division
 *
 * IMPORTANT:
 * ----------
 * - NO legacy integer hashes
 * - NO multiprobe buckets
 * - Prefix matching ONLY (BitSet-based)
 *
 * This enforces correctness and prevents silent recall collapse.
 */
public final class QueryToken implements Serializable {

    // =====================================================
    // MSANNP Bit Codes: [table][division]
    // =====================================================
    private BitSet[][] bitCodes;

    // =====================================================
    // Encrypted query material
    // =====================================================
    private final byte[] iv;
    private final byte[] encryptedQuery;

    // =====================================================
    // Query parameters
    // =====================================================
    private final int topK;
    private final int numTables;
    private final int dimension;
    private final int version;
    private final int lambda;
    private final String encryptionContext;

    // =====================================================
    // Constructor
    // =====================================================
    public QueryToken(
            BitSet[][] bitCodes,
            byte[] iv,
            byte[] encryptedQuery,
            int topK,
            int numTables,
            int dimension,
            int version,
            int lambda,
            String encryptionContext
    ) {
        this.bitCodes = deepClone(bitCodes);

        this.iv = Objects.requireNonNull(iv).clone();
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery).clone();

        this.topK = Math.max(1, topK);
        this.numTables = Math.max(1, numTables);
        this.dimension = dimension;
        this.version = version;
        this.lambda = lambda;
        this.encryptionContext = encryptionContext;
    }

    // =====================================================
    // BitCode access (MSANNP core)
    // =====================================================
    public BitSet[][] getBitCodes() {
        return deepClone(bitCodes);
    }

    public void setBitCodes(BitSet[][] bitCodes) {
        this.bitCodes = deepClone(bitCodes);
    }

    // =====================================================
    // Encrypted payload access
    // =====================================================
    public byte[] getIv() {
        return iv.clone();
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery.clone();
    }

    // =====================================================
    // Metadata / diagnostics
    // =====================================================
    public int getTopK() {
        return topK;
    }

    public int getNumTables() {
        return numTables;
    }

    public int getDimension() {
        return dimension;
    }

    public int getVersion() {
        return version;
    }

    public int getLambda() {
        return lambda;
    }

    public String getEncryptionContext() {
        return encryptionContext;
    }

    // =====================================================
    // Helpers
    // =====================================================
    private static BitSet[][] deepClone(BitSet[][] src) {
        if (src == null) return null;

        BitSet[][] out = new BitSet[src.length][];
        for (int i = 0; i < src.length; i++) {
            if (src[i] == null) {
                out[i] = null;
                continue;
            }
            out[i] = new BitSet[src[i].length];
            for (int j = 0; j < src[i].length; j++) {
                out[i][j] = (src[i][j] == null) ? null : (BitSet) src[i][j].clone();
            }
        }
        return out;
    }
}
