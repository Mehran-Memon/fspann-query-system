package com.fspann.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * Low-level AES-GCM encryption/decryption utilities.
 */
public final class EncryptionUtils {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    public static final int GCM_IV_LENGTH = 12; // Recommended length for AES-GCM
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private EncryptionUtils() {}

    /**
     * Securely generates a 12-byte IV for AES-GCM.
     */
    public static byte[] generateIV() {
        byte[] iv = new byte[GCM_IV_LENGTH];
        SECURE_RANDOM.nextBytes(iv);
        return iv;
    }

    /**
     * Encrypts a double[] vector using AES-GCM.
     *
     * @param vector the plaintext vector
     * @param iv initialization vector (12 bytes)
     * @param key AES SecretKey (256-bit recommended)
     * @return encrypted byte[]
     * @throws GeneralSecurityException if encryption fails
     */
    public static byte[] encryptVector(double[] vector, byte[] iv, SecretKey key) throws GeneralSecurityException {
        validateParams(vector, iv, key);
        byte[] plaintext = doubleArrayToBytes(vector);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
        try {
            return cipher.doFinal(plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0); // Clear sensitive data
        }
    }

    /**
     * Decrypts an encrypted byte[] to double[].
     *
     * @param ciphertext the encrypted bytes
     * @param iv the initialization vector used during encryption
     * @param key the SecretKey used for encryption
     * @return the decrypted vector
     * @throws GeneralSecurityException if decryption fails
     */
    public static double[] decryptVector(byte[] ciphertext, byte[] iv, SecretKey key) throws GeneralSecurityException {
        validateParams(ciphertext, iv, key);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
        byte[] plaintext = cipher.doFinal(ciphertext);
        try {
            return bytesToDoubleArray(plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0); // Prevent memory leak of sensitive data
        }
    }

    // --- Helper Methods ---

    private static byte[] doubleArrayToBytes(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) buffer.putDouble(d);
        return buffer.array();
    }

    private static double[] bytesToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] result = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < result.length; i++) result[i] = buffer.getDouble();
        return result;
    }

    private static void validateParams(Object input, byte[] iv, SecretKey key) {
        Objects.requireNonNull(input, "Input vector or ciphertext cannot be null");
        Objects.requireNonNull(iv, "IV cannot be null");
        Objects.requireNonNull(key, "SecretKey cannot be null");
        if (iv.length != GCM_IV_LENGTH) throw new IllegalArgumentException("IV length must be " + GCM_IV_LENGTH + " bytes");
    }
}
