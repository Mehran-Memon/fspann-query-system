package com.fspann.common;

import java.io.Serializable;
import java.util.*;
import java.util.BitSet;

/**
 * Unified query token supporting both legacy single-bucket set and per-table expansions.
 * Preferred path: supply per-table buckets and precomputed codes (one BitSet per division).
 */
public class QueryToken implements Serializable {
    private static final long serialVersionUID = 1L;

    // --- new preferred fields ---
    private final List<List<Integer>> tableBuckets;  // per-table expansions
    private final int numTables;
    private final BitSet[] codes;                    // one BitSet per division (ℓ)

    // --- legacy fields (kept for compatibility) ---
    @Deprecated
    private final List<Integer> candidateBuckets;
    @Deprecated
    private final int shardId; // ignored by new path

    // --- shared fields ---
    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final double[] queryVector; // legacy plaintext for tests only
    private final int topK;
    private final String encryptionContext;
    private final int dimension;
    private final int version;

    // ---- Preferred constructor (per-table + codes) ----
    public QueryToken(List<List<Integer>> tableBuckets,
                      BitSet[] codes,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] queryVector,     // may be null in production (server won’t use)
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int version) {
        this.tableBuckets = deepUnmodifiableBuckets(
                Objects.requireNonNull(tableBuckets, "tableBuckets"));
        this.numTables = positive(numTables, "numTables");

        // Deep-copy BitSets for immutability
        this.codes = (codes == null) ? null : deepCloneCodes(codes);

        this.iv = Objects.requireNonNull(iv, "iv").clone();
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery, "encryptedQuery").clone();
        this.queryVector = (queryVector == null ? null : queryVector.clone());

        this.topK = positive(topK, "topK");
        this.encryptionContext = Objects.requireNonNullElseGet(
                encryptionContext, () -> "epoch_" + version + "_dim_" + dimension);
        this.dimension = positive(dimension, "dimension");
        this.version = version;

        // legacy
        this.candidateBuckets = List.of();
        this.shardId = 0;

        if (iv.length != 12) throw new IllegalArgumentException("iv must be 12 bytes for AES-GCM");
        if (this.tableBuckets.stream().anyMatch(List::isEmpty)) {
            throw new IllegalArgumentException("Each table must have at least one bucket");
        }
        // codes may be null for legacy clients; server code should handle null defensively.
    }

    // ---- Legacy constructor (kept so old tests & callers continue to compile) ----
    /** @deprecated Prefer the constructor that also accepts BitSet[] codes (one per division). */
    @Deprecated
    public QueryToken(List<List<Integer>> tableBuckets,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] queryVector,      // may be null in production (server won’t use)
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int version) {
        this(
                tableBuckets,
                /* codes = */ null,        // legacy path: no per-division codes provided
                iv,
                encryptedQuery,
                queryVector,
                topK,
                numTables,
                encryptionContext,
                dimension,
                version
        );
    }


    private static BitSet[] deepCloneCodes(BitSet[] src) {
        BitSet[] out = new BitSet[src.length];
        for (int i = 0; i < src.length; i++) {
            out[i] = (src[i] == null) ? null : (BitSet) src[i].clone();
        }
        return out;
    }

    /** One BitSet per division (length == ℓ). May be null for legacy tokens. */
    public BitSet[] getCodes() {
        if (codes == null) return null;
        return deepCloneCodes(codes);
    }
    // ---- helpers ----
    public boolean hasPerTable() { return tableBuckets != null && !tableBuckets.isEmpty(); }

    public List<List<Integer>> getTableBuckets() {
        return tableBuckets == null ? List.of() : tableBuckets;
    }

    /** No-arg compatibility: use stored numTables for legacy tokens. */
    public List<List<Integer>> getTableBucketsOrLegacy() {
        if (hasPerTable()) return tableBuckets;
        List<List<Integer>> out = new ArrayList<>(this.numTables);
        for (int i = 0; i < this.numTables; i++) {
            out.add(List.copyOf(candidateBuckets));
        }
        return out;
    }

    private static List<List<Integer>> deepUnmodifiableBuckets(List<List<Integer>> src) {
        List<List<Integer>> out = new ArrayList<>(src.size());
        for (List<Integer> l : src) {
            out.add(List.copyOf(Objects.requireNonNull(l, "table bucket list must not be null")));
        }
        return Collections.unmodifiableList(out);
    }

    private static int positive(int v, String name) {
        if (v <= 0) throw new IllegalArgumentException(name + " must be > 0");
        return v;
    }

    // ---- getters (legacy names preserved) ----
    @Deprecated public List<Integer> getCandidateBuckets() { return candidateBuckets; }
    public List<List<Integer>> getPerTableBuckets() { return getTableBuckets(); }

    public byte[] getIv() { return iv.clone(); }
    public byte[] getEncryptedQuery() { return encryptedQuery.clone(); }

    /** Server should avoid using plaintext; kept for tests. */
    @Deprecated public double[] getPlaintextQuery() { return queryVector == null ? null : queryVector.clone(); }
    public double[] getQueryVector() { return getPlaintextQuery(); }

    public int getTopK() { return topK; }
    public int getNumTables() { return numTables; }
    public String getEncryptionContext() { return encryptionContext; }
    public int getDimension() { return dimension; }
    public int getVersion() { return version; }

    @Deprecated public int getShardId() { return shardId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryToken other)) return false;

        // Compare stable identity only (exclude iv/ciphertext)
        return topK == other.topK
                && numTables == other.numTables
                && dimension == other.dimension
                && version == other.version
                && Objects.equals(encryptionContext, other.encryptionContext)
                && Objects.equals(tableBuckets, other.tableBuckets)
                && Objects.equals(candidateBuckets, other.candidateBuckets)
                && Arrays.equals(queryVector, other.queryVector);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                topK, numTables, encryptionContext, dimension, version,
                tableBuckets, candidateBuckets
        );
        result = 31 * result + Arrays.hashCode(queryVector);
        return result;
    }

    @Override public String toString() {
        return "QueryToken{topK=" + topK + ", numTables=" + numTables +
                ", dim=" + dimension + ", version=" + version +
                ", perTable=" + (hasPerTable() ? tableBuckets.size() : 0) +
                ", hasCodes=" + (codes != null) + "}";
    }
}
