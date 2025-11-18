package com.fspann.common;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Unified query token supporting:
 *  - per-table LSH bucket expansions (legacy multiprobe path)
 *  - precomputed codes (one BitSet per division) for partitioned indexing
 *
 * NOTE: This token is encryption-only on the wire:
 *  - It carries IV + encryptedQuery bytes.
 *  - It does NOT carry the plaintext query vector.
 */
public class QueryToken implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<List<Integer>> tableBuckets;  // per-table expansions
    private final int numTables;
    private final BitSet[] codes;                    // one BitSet per division (ℓ)
    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final int topK;
    private final String encryptionContext;
    private final int dimension;
    private final int version;

    public QueryToken(List<List<Integer>> tableBuckets,
                      BitSet[] codes,
                      byte[] iv,
                      byte[] encryptedQuery,
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int version) {
        this.tableBuckets = List.copyOf(Objects.requireNonNull(tableBuckets, "tableBuckets"));
        this.numTables = Math.max(1, numTables);
        this.codes = (codes == null) ? null : deepCloneCodes(codes);
        this.iv = Objects.requireNonNull(iv, "iv").clone();
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery, "encryptedQuery").clone();
        this.topK = Math.max(1, topK);
        this.encryptionContext = Objects.requireNonNullElseGet(
                encryptionContext,
                () -> "epoch_" + version + "_dim_" + dimension
        );
        this.dimension = Math.max(1, dimension);
        this.version = version;

        if (this.iv.length != 12) {
            throw new IllegalArgumentException("iv must be 12 bytes for AES-GCM");
        }
    }

    private static BitSet[] deepCloneCodes(BitSet[] src) {
        BitSet[] out = new BitSet[src.length];
        for (int i = 0; i < src.length; i++) {
            out[i] = (src[i] == null) ? null : (BitSet) src[i].clone();
        }
        return out;
    }

    public List<List<Integer>> getTableBuckets() {
        return tableBuckets;
    }

    public BitSet[] getCodes() {
        return (codes == null) ? null : deepCloneCodes(codes);
    }

    public byte[] getIv() {
        return iv.clone();
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery.clone();
    }

    public int getTopK() {
        return topK;
    }

    public int getNumTables() {
        return numTables;
    }

    public String getEncryptionContext() {
        return encryptionContext;
    }

    public int getDimension() {
        return dimension;
    }

    public int getVersion() {
        return version;
    }
}
