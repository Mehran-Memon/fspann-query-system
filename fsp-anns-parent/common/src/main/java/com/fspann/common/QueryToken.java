package com.fspann.common;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Unified query token supporting:
 *  - per-table LSH bucket expansions (legacy multiprobe path)
 *  - per-table precomputed codes (BitSet per division) for partitioned indexing (mSANNP)
 *
 * NOTE: This token is encryption-only on the wire:
 *  - It carries IV + encryptedQuery bytes.
 *  - It does NOT carry the plaintext query vector.
 */
public class QueryToken implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<List<Integer>> tableBuckets;  // legacy
    private final int numTables;

    // mSANNP: [table][division]
    private final BitSet[][] codesByTable;

    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final int topK;
    private final String encryptionContext;
    private final int dimension;
    private final int version;
    private final int lambda;

    public QueryToken(
            List<List<Integer>> tableBuckets,
            BitSet[][] codesByTable,
            byte[] iv,
            byte[] encryptedQuery,
            int topK,
            int numTables,
            String encryptionContext,
            int dimension,
            int version,
            int lambda
    ) {
        this.tableBuckets = List.copyOf(Objects.requireNonNull(tableBuckets, "tableBuckets"));
        this.numTables = Math.max(1, numTables);
        this.codesByTable = deepCloneCodes2D(codesByTable);

        this.iv = Objects.requireNonNull(iv, "iv").clone();
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery, "encryptedQuery").clone();
        this.topK = Math.max(1, topK);

        this.encryptionContext = Objects.requireNonNullElseGet(
                encryptionContext,
                () -> "epoch_" + version + "_dim_" + dimension
        );
        this.dimension = Math.max(1, dimension);
        this.version = version;
        this.lambda = Math.max(1, lambda);

        if (this.iv.length != 12) {
            throw new IllegalArgumentException("iv must be 12 bytes for AES-GCM");
        }
    }

    private static BitSet[][] deepCloneCodes2D(BitSet[][] src) {
        if (src == null) return null;
        BitSet[][] out = new BitSet[src.length][];
        for (int t = 0; t < src.length; t++) {
            BitSet[] row = src[t];
            if (row == null) {
                out[t] = null;
                continue;
            }
            out[t] = new BitSet[row.length];
            for (int d = 0; d < row.length; d++) {
                out[t][d] = (row[d] == null) ? null : (BitSet) row[d].clone();
            }
        }
        return out;
    }

    public List<List<Integer>> getTableBuckets() {
        return tableBuckets;
    }

    // mSANNP: per-table codes
    public BitSet[][] getCodesByTable() {
        return codesByTable;
    }

    // Legacy: returns table-0 codes only
    public BitSet[] getCodes() {
        if (codesByTable == null || codesByTable.length == 0) return null;
        BitSet[] row = codesByTable[0];
        if (row == null) return null;
        BitSet[] out = new BitSet[row.length];
        for (int i = 0; i < row.length; i++) out[i] = (row[i] == null) ? null : (BitSet) row[i].clone();
        return out;
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

    public int getLambda() {
        return lambda;
    }

    public int estimateSerializedSizeBytes() {
        int sum = 0;
        if (codesByTable != null) {
            for (BitSet[] row : codesByTable) {
                if (row == null) continue;
                for (BitSet bs : row) sum += (bs == null ? 1 : (bs.toByteArray().length + 1));
            }
        }
        return sum;
    }
}
