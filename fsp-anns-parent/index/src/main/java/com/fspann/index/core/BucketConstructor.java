package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @deprecated
 * This class previously offered cosine-based bucket merging and "fake point" padding.
 * Those behaviors are NOT part of the SANNP / mSANNP architecture and can harm accuracy.
 * Replace all usages with the paper-aligned pipeline:
 *
 *   1) LSH-based coding (Algorithm-1) to produce the linear code C,
 *   2) Greedy partition over the sorted codes with width w (Algorithm-2),
 *   3) Tag-based lookup (Algorithm-3) returning one or two subsets per query,
 *   4) Client-side kNN over the returned (≤ 2w) plaintext candidates.
 *
 * See: SANNP model & algorithms (coding, greedy partition, tag query). :contentReference[oaicite:0]{index=0}
 * Also see your dissertation proposal sections aligning the framework with LSH + Greedy Partition. :contentReference[oaicite:1]{index=1}
 *
 * This class is retained only for source/binary compatibility during migration.
 * All former methods now throw UnsupportedOperationException with guidance.
 */
@Deprecated
public final class BucketConstructor {

    private BucketConstructor() {
        // no instances
    }

    /**
     * @deprecated Do not use. Cosine-based greedy merging of plaintext vectors is not part of SANNP.
     * Use the paper-aligned build:
     *   - Compute C(v) via LSH coding (Algorithm-1),
     *   - Sort by C and run GreedyPartitioner (Algorithm-2) to produce subsets SD_i and map index I.
     *
     * If you need a placeholder during refactors, call {@link #noOp()} to obtain an empty, immutable list.
     *
     * @throws UnsupportedOperationException always
     */
    @Deprecated
    public static List<List<EncryptedPoint>> greedyMerge(
            List<EncryptedPoint> points,
            int maxSize,
            Function<EncryptedPoint, double[]> unpackFunc) {
        throw new UnsupportedOperationException(
                "BucketConstructor.greedyMerge(...) has been removed.\n" +
                        "Migrate to the SANNP build: C(v) coding -> greedy partition (Algorithm-2) -> tag index. " +
                        "[ref: SANNP paper Algorithms 1–3]"); // :contentReference[oaicite:2]{index=2}
    }

    /**
     * @deprecated Do not use. Fake-point padding must NOT contaminate the searchable in-memory index.
     * Size-hiding, if required, should be implemented at the storage layer only, and excluded from
     * candidate generation & evaluation.
     *
     * @throws UnsupportedOperationException always
     */
    @Deprecated
    public static List<List<EncryptedPoint>> applyFake(
            List<List<EncryptedPoint>> buckets,
            int cap,
            EncryptedPoint template) {
        throw new UnsupportedOperationException(
                "BucketConstructor.applyFake(...) has been removed.\n" +
                        "Do not insert fake points into the searchable index. If size-hiding is required, " +
                        "perform it at the storage layer and filter from query results.");
    }

    /**
     * Utility: returns an empty, immutable list. Helpful as a drop-in while removing
     * old call sites that expected a list of buckets from this class.
     */
    public static <T> List<List<T>> noOp() {
        return Collections.emptyList();
    }

    /**
     * Helper for legacy clean-up: detect the old fake-ID convention ("FAKE_*").
     * During migration, you can filter such points from any residual candidate set.
     */
    public static boolean isLegacyFakeId(String id) {
        return id != null && id.startsWith("FAKE_");
    }

    /**
     * Validate an {@link EncryptedPoint} reference (non-null, minimal fields). This is only
     * provided to make refactors less noisy while deleting old BucketConstructor usages.
     */
    public static void requireValid(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        // Minimal sanity checks; full validation belongs in indexing services.
        if (pt.getVectorLength() <= 0) {
            throw new IllegalArgumentException("EncryptedPoint.vectorLength must be > 0");
        }
    }
}
