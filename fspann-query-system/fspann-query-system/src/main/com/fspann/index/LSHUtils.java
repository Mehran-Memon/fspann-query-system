package com.fspann.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LSHUtils {

    private static final Logger logger = LoggerFactory.getLogger(LSHUtils.class);

    /**
     * Computes the critical values (bucket boundaries) for Even LSH using the dataset.
     * @param lsh The Even LSH instance used for projection.
     * @param numBuckets The number of buckets to divide the data into.
     * @param key The encryption key for decrypting data points, or null if not encrypted.
     * @return The computed critical values (bucket boundaries).
     * @throws Exception if there is an error during decryption.
     */
    public static double[] computeCriticalValues(List<byte[]> encryptedData, EvenLSH lsh, int numBuckets, SecretKey key) throws Exception {
        List<Double> projections = new ArrayList<>();
        for (byte[] encryptedPoint : encryptedData) {
            double[] decryptedPoint;
            if (key != null) {
                // Split into IV and ciphertext (assuming IV is prepended to ciphertext)
                ByteBuffer buffer = ByteBuffer.wrap(encryptedPoint);
                byte[] iv = new byte[EncryptionUtils.GCM_IV_LENGTH];
                buffer.get(iv);
                byte[] ciphertext = new byte[buffer.remaining()];
                buffer.get(ciphertext);

                decryptedPoint = EncryptionUtils.decryptVector(ciphertext, iv, key);
            } else {
                decryptedPoint = BucketConstructor.byteToDoubleArray(encryptedPoint);
            }

            double projection = lsh.project(decryptedPoint);
            projections.add(projection);
        }

        // Sort projections and compute critical values (quantiles)
        projections.sort(Double::compare);
        double[] criticalValues = new double[numBuckets];
        for (int i = 1; i <= numBuckets; i++) {
            int index = (int) Math.floor(i * projections.size() / (double) (numBuckets + 1));  // Quantile computation
            criticalValues[i - 1] = projections.get(Math.min(index, projections.size() - 1));
        }

        // Log the critical values for tracking
        logger.info("Computed critical values: ");
        for (double value : criticalValues) {
            logger.info(String.valueOf(value));
        }

        return criticalValues;
    }}