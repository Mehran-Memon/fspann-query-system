package com.fspann.index;

import java.nio.ByteBuffer;
import java.util.List;
import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BucketConstructor {

    private static final Logger log = LoggerFactory.getLogger(BucketConstructor.class);
    private static final double FAKE_MARK = -1.0;           // recognise fake points
    private static final double TAU_ANGLE = 0.07;            // cosθ gap threshold
    private static final int    FAKE_CAP  = 10;             // ψ
    byte[] iv = EncryptionUtils.generateIV();

    private BucketConstructor() {}  // util class

    /* -----------------------------------------------------------
       Greedy merge with cosine distance
       ----------------------------------------------------------- */
    public static List<List<byte[]>> greedyMergeCosine(
            List<double[]> data,
            EvenLSH lsh,
            int maxBucketSize,
            SecretKey key) throws Exception {

        // 1. sort by projection onto the primary hyper-plane (same vector in lsh)
        data.sort(Comparator.comparingDouble(v -> -lsh.project(v)));


        List<List<byte[]>> buckets = new ArrayList<>();
        List<byte[]> current = new ArrayList<>();
        double[] ref = null;

        for (double[] vec : data) {
            if (current.isEmpty()) {           // start new bucket
                current.add(enc(vec, key));
                ref = vec;
                continue;
            }
            double cosGap = cosine(vec, ref);
            if (cosGap >= 1 - TAU_ANGLE && current.size() < maxBucketSize) {
                current.add(enc(vec, key));
            } else {
                buckets.add(current);
                current = new ArrayList<>();
                current.add(enc(vec, key));
                ref = vec;
            }
        }
        if (!current.isEmpty()) buckets.add(current);
        logSizes("After greedyMergeCosine", buckets);
        return buckets;
    }

    /* -----------------------------------------------------------
       Fake-addition (Algorithm 2)
       ----------------------------------------------------------- */
    public static List<List<byte[]>> applyFakeAddition(List<List<byte[]>> buckets,
                                                       int targetSize,
                                                       SecretKey key,
                                                       int dimension) throws Exception {

        int max = buckets.stream().mapToInt(List::size).max().orElse(targetSize);
        max = Math.max(max, targetSize);               // ensure ≥ target

        for (List<byte[]> B : buckets) {
            int need = max - B.size();
            int toAdd = Math.min(FAKE_CAP, Math.max(0, need));
            for (int i = 0; i < toAdd; i++) {
                double[] fake = randomFake(dimension);
                B.add(enc(fake, key));
            }
        }
        logSizes("After fake-addition", buckets);
        return buckets;
    }


    /* ----------------------------------------------------------- helpers */

    private static byte[] enc(double[] v, SecretKey k) throws Exception {
        if (k == null) {
            return doublesToBytes(v);
        } else {
            // Generate a new IV for each encryption
            byte[] iv = EncryptionUtils.generateIV();

            // Encrypt with the new IV and the session key
            byte[] ciphertext = EncryptionUtils.encryptVector(v, iv, k);

            // Combine IV and ciphertext for storage (IV needed for decryption)
            return ByteBuffer.allocate(iv.length + ciphertext.length)
                    .put(iv)
                    .put(ciphertext)
                    .array();
        }
    }

//    public static double[] dec(byte[] encryptedData, SecretKey k) throws Exception {
//        if (k == null) {
//            return bytesToDoubles(encryptedData);
//        }
//
//        ByteBuffer buffer = ByteBuffer.wrap(encryptedData);
//        byte[] iv = new byte[EncryptionUtils.GCM_IV_LENGTH];
//        buffer.get(iv);
//        byte[] ciphertext = new byte[buffer.remaining()];
//        buffer.get(ciphertext);
//
//        return EncryptionUtils.decryptVector(ciphertext, iv, k);
//    }

    private static double dot(double[] a, double[] b) {
        double s = 0; for (int i = 0; i < a.length; i++) s += a[i] * b[i]; return s;
    }
    private static double cosine(double[] a, double[] b) { return dot(a, b) / (norm(a) * norm(b)); }
    private static double norm(double[] v) { return Math.sqrt(dot(v, v)); }

    private static double[] randomFake(int d) {
        double[] f = new double[d];
        Arrays.fill(f, FAKE_MARK + Math.random() * 0.001);   // tiny noise
        return f;
    }
    private static byte[] doublesToBytes(double[] v) {
        ByteBuffer buf = ByteBuffer.allocate(v.length * Double.BYTES);
        for (double x : v) buf.putDouble(x);
        return buf.array();
    }
    private static void logSizes(String tag, List<List<byte[]>> B) {
        StringBuilder sb = new StringBuilder(tag).append(" bucket sizes: ");
        for (List<byte[]> b : B) sb.append(b.size()).append(' ');
        log.info(sb.toString());
    }

    /**
     * Converts a byte array into a double array.
     * @param bytes The byte array to convert.
     * @return The converted double array.
     */
    public static double[] byteToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();  // Extract each double from the byte buffer
        }
        return vector;
    }

}

