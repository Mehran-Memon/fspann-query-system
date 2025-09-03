package com.fspann.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Low-level AES-GCM encryption/decryption utilities.
 */
public final class EncryptionUtils {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    public static final int GCM_IV_LENGTH = 12; // Recommended length for AES-GCM
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final Logger logger = LoggerFactory.getLogger(EncryptionUtils.class);

    private EncryptionUtils() {}

    /** Securely generates a 12-byte IV for AES-GCM. */
    public static byte[] generateIV() {
        byte[] iv = new byte[GCM_IV_LENGTH];
        SECURE_RANDOM.nextBytes(iv);
        return iv;
    }

    /** Encrypts a double[] vector using AES-GCM (no AAD). */
    public static byte[] encryptVector(double[] vector, byte[] iv, SecretKey key) throws GeneralSecurityException {
        return encryptVectorWithAad(vector, iv, key, null);
    }

    /** Decrypts AES-GCM ciphertext to double[] (no AAD). */
    public static double[] decryptVector(byte[] ciphertext, byte[] iv, SecretKey key) throws GeneralSecurityException {
        return decryptVectorWithAad(ciphertext, iv, key, null);
    }

    /** Encrypts a double[] vector using AES-GCM with optional AAD. */
    public static byte[] encryptVectorWithAad(double[] vector, byte[] iv, SecretKey key, byte[] aad) throws GeneralSecurityException {
        validateParams(vector, iv, key);
        if (vector.length == 0) throw new IllegalArgumentException("Vector cannot be empty");
        for (double v : vector) {
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException("Vector contains invalid values (NaN or Infinite)");
            }
        }
        byte[] plaintext = doubleArrayToBytes(vector);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
        if (aad != null && aad.length > 0) cipher.updateAAD(aad);
        try {
            logger.debug("Encrypting vector of size {} with IV: {}", vector.length, Base64.getEncoder().encodeToString(iv));
            return cipher.doFinal(plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0); // Clear sensitive data
        }
    }

    /** Decrypts AES-GCM ciphertext with optional AAD to double[]. */
    public static double[] decryptVectorWithAad(byte[] ciphertext, byte[] iv, SecretKey key, byte[] aad) throws GeneralSecurityException {
        validateParams(ciphertext, iv, key);
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.DECRYPT_MODE, key, spec);
        if (aad != null && aad.length > 0) cipher.updateAAD(aad);
        byte[] decrypted = cipher.doFinal(ciphertext);
        try {
            ByteBuffer buffer = ByteBuffer.wrap(decrypted);
            double[] result = new double[decrypted.length / 8];
            for (int i = 0; i < result.length; i++) {
                result[i] = buffer.getDouble();
            }
            return result;
        } finally {
            Arrays.fill(decrypted, (byte) 0); // Clear sensitive decrypted data
        }
    }

    private static byte[] doubleArrayToBytes(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) buffer.putDouble(d);
        return buffer.array();
    }

    @SuppressWarnings("unused")
    private static double[] bytesToDoubleArray(byte[] bytes) {
        if (bytes.length % Double.BYTES != 0) {
            throw new IllegalArgumentException("Invalid byte array length for double array conversion");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] result = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < result.length; i++) {
            result[i] = buffer.getDouble();
        }
        return result;
    }

    private static void validateParams(Object input, byte[] iv, SecretKey key) {
        Objects.requireNonNull(input, "Input vector or ciphertext cannot be null");
        Objects.requireNonNull(iv, "IV cannot be null");
        Objects.requireNonNull(key, "SecretKey cannot be null");
        if (iv.length != GCM_IV_LENGTH) {
            throw new IllegalArgumentException("IV length must be " + GCM_IV_LENGTH + " bytes");
        }
        if (input instanceof byte[] && ((byte[]) input).length < GCM_TAG_LENGTH_BITS / 8) {
            throw new IllegalArgumentException("Ciphertext too short");
        }
    }
}
