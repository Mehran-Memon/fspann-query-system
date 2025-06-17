package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides static methods for secure and realistic bucket construction in high-dimensional encrypted ANN settings.
 */
public final class BucketConstructor {
    private static final SecureRandom RANDOM = new SecureRandom();

    private BucketConstructor() {}

    /**
     * Greedily merges encrypted points based on cosine similarity to an evolving reference.
     * @param points list of encrypted points
     * @param maxSize maximum bucket size
     * @param unpackFunc function to extract plaintext vector from EncryptedPoint
     * @return list of similarity-aware merged buckets
     */
    public static List<List<EncryptedPoint>> greedyMerge(
            List<EncryptedPoint> points,
            int maxSize,
            Function<EncryptedPoint, double[]> unpackFunc) {

        List<List<EncryptedPoint>> buckets = new ArrayList<>();
        Set<EncryptedPoint> unassigned = new HashSet<>(points);

        while (!unassigned.isEmpty()) {
            List<EncryptedPoint> bucket = new ArrayList<>();
            Iterator<EncryptedPoint> it = unassigned.iterator();
            EncryptedPoint seed = it.next();
            it.remove();
            bucket.add(seed);

            double[] refVec = unpackFunc.apply(seed);

            List<EncryptedPoint> sorted = unassigned.stream()
                    .sorted(Comparator.comparingDouble(pt -> -cosineSimilarity(refVec, unpackFunc.apply(pt))))
                    .limit(maxSize - 1)
                    .collect(Collectors.toList());

            bucket.addAll(sorted);
            unassigned.removeAll(sorted);
            buckets.add(bucket);
        }
        return buckets;
    }

    /**
     * Pads each bucket with randomized fake points derived from a template.
     * @param buckets list of real buckets
     * @param cap desired uniform size
     * @param template source encrypted point
     * @return buckets padded with unique-looking fake points
     */
    public static List<List<EncryptedPoint>> applyFake(
            List<List<EncryptedPoint>> buckets,
            int cap,
            EncryptedPoint template) {

        for (List<EncryptedPoint> bucket : buckets) {
            int shard = template.getShardId();
            int version = template.getVersion();
            while (bucket.size() < cap) {
                byte[] newIv = new byte[template.getIv().length];
                RANDOM.nextBytes(newIv);

                byte[] fakeCiphertext = Arrays.copyOf(template.getCiphertext(), template.getCiphertext().length);
                RANDOM.nextBytes(fakeCiphertext); // overwrite ciphertext randomness

                EncryptedPoint fake = new EncryptedPoint(
                        "FAKE_" + UUID.randomUUID(),
                        shard,
                        newIv,
                        fakeCiphertext,
                        version,
                        template.getVectorLength()
                );
                bucket.add(fake);
            }
        }
        return buckets;
    }

    // Cosine similarity (assumes vectors are not null or empty)
    private static double cosineSimilarity(double[] a, double[] b) {
        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB) + 1e-9);
    }
}
