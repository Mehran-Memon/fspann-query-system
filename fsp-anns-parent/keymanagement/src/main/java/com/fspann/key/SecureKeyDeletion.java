package com.fspann.key;

import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Secure key deletion utilities ensuring cryptographic material is overwritten.
 */
public final class SecureKeyDeletion {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private SecureKeyDeletion() {}

    /**
     * Securely wipes a SecretKey by overwriting its encoded bytes.
     * Uses multiple passes with random data followed by zeros.
     */
    public static void wipeKey(SecretKey key) {
        if (key == null) return;

        try {
            byte[] encoded = key.getEncoded();
            if (encoded != null) {
                // Pass 1: Random overwrite
                SECURE_RANDOM.nextBytes(encoded);

                // Pass 2: Zeros
                Arrays.fill(encoded, (byte) 0);

                // Pass 3: Ones
                Arrays.fill(encoded, (byte) 0xFF);

                // Final pass: Zeros
                Arrays.fill(encoded, (byte) 0);
            }
        } catch (Exception e) {
            // Log but don't throw - best effort
        }
    }

    /**
     * Wipes a byte array containing sensitive cryptographic material.
     */
    public static void wipeBytes(byte[] data) {
        if (data == null || data.length == 0) return;

        SECURE_RANDOM.nextBytes(data);
        Arrays.fill(data, (byte) 0);
        Arrays.fill(data, (byte) 0xFF);
        Arrays.fill(data, (byte) 0);
    }
}