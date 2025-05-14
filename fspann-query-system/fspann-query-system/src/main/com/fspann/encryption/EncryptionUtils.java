package com.fspann.encryption;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;

public final class EncryptionUtils {
    private static final Logger logger = LoggerFactory.getLogger(EncryptionUtils.class);

    // Constants
    public static final String TRANSFORMATION = "AES/GCM/NoPadding";
    public static final int GCM_IV_LENGTH = 12; // Standard IV length for GCM
    public static final int GCM_TAG_LENGTH = 128; // 128-bit authentication tag
    public static final int DOUBLE_BYTES = Double.BYTES;

    // Thread-local cipher instances
    private static final ThreadLocal<Cipher> cipherThreadLocal = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(TRANSFORMATION);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Cipher instance", e);
        }
    });
    private EncryptionUtils() {} // Prevent instantiation

    // Method to generate a random Initialization Vector (IV)
    public static byte[] generateIV() {
        byte[] iv = new byte[12]; // AES-GCM requires 12-byte IV
        new java.security.SecureRandom().nextBytes(iv); // Fill the IV with random bytes
        return iv;
    }

    // Method to encrypt a vector with AES/GCM/NoPadding
// Method to encrypt a vector with AES/GCM/NoPadding
    public static byte[] encryptVector(double[] vector, byte[] iv, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv); // 128-bit authentication tag
        cipher.init(Cipher.ENCRYPT_MODE, key, gcmSpec);

        // Convert the vector into bytes
        byte[] data = Arrays.toString(vector).getBytes("UTF-8"); // Example conversion

        byte[] encryptedData = cipher.doFinal(data);
        byte[] result = new byte[iv.length + encryptedData.length];
        System.arraycopy(iv, 0, result, 0, iv.length); // Add the IV at the beginning
        System.arraycopy(encryptedData, 0, result, iv.length, encryptedData.length); // Add encrypted data after the IV
        return result;
    }

    // Method to decrypt a vector with AES/GCM/NoPadding
    public static double[] decryptVector(byte[] encryptedData, byte[] iv, SecretKey key) throws Exception {
        byte[] cipherText = Arrays.copyOfRange(encryptedData, iv.length, encryptedData.length);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv); // 128-bit authentication tag
        cipher.init(Cipher.DECRYPT_MODE, key, gcmSpec);

        byte[] decryptedData = cipher.doFinal(cipherText);

        // Convert the decrypted data back into the original vector (e.g., converting from a string)
        String dataString = new String(decryptedData, "UTF-8"); // Example conversion
        String[] vectorString = dataString.substring(1, dataString.length() - 1).split(", ");
        double[] vector = new double[vectorString.length];
        for (int i = 0; i < vectorString.length; i++) {
            vector[i] = Double.parseDouble(vectorString[i]);
        }
        return vector;
    }

    public static Pair<byte[], byte[]> reEncryptData(byte[] encryptedData, byte[] iv,
                                                     SecretKey oldKey, SecretKey newKey) throws Exception {
        validateEncryptionInputs(encryptedData, iv, oldKey);

        if (newKey == null) {
            throw new IllegalArgumentException("New key cannot be null");
        }

        // Decrypt data using the old key with the provided IV
        double[] decrypted = decryptVector(encryptedData, iv, oldKey);

        if (decrypted == null) {
            throw new Exception("Decryption failed with the old key.");
        }

        // Generate new IV for re-encryption
        byte[] newIv = generateIV();

        // Re-encrypt with the new key and the newly generated IV
        byte[] newCiphertext = encryptVector(decrypted, iv, newKey);

        // Return re-encrypted data and new IV
        return Pair.of(newCiphertext, newIv);
    }

    private static byte[] doubleArrayToByteArray(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * DOUBLE_BYTES);
        try {
            for (double d : vector) {
                buffer.putDouble(d);
            }
            return buffer.array();
        } finally {
            if (buffer != null && buffer.hasArray()) {
                Arrays.fill(buffer.array(), (byte) 0);
            }
        }
    }

    private static double[] byteArrayToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / DOUBLE_BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();
        }
        return vector;
    }

    private static void validateEncryptionInputs(byte[] data, byte[] iv, SecretKey key) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }
        if (iv == null || iv.length != GCM_IV_LENGTH) {
            throw new IllegalArgumentException("IV must be exactly " + GCM_IV_LENGTH + " bytes");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
    }

    // Additional utility methods
    public static boolean isEncryptedWith(byte[] ciphertext, byte[] iv, SecretKey key) {
        try {
            // Decrypt the ciphertext with the provided IV and key
            decryptVector(ciphertext, iv, key);  // This now includes both ciphertext and iv
            return true;  // If decryption is successful, it was encrypted with the provided key
        } catch (Exception e) {
            return false;  // If decryption fails, the data was not encrypted with the given key
        }
    }


    public static int getEncryptedSize(int vectorLength) {
        return vectorLength * DOUBLE_BYTES + GCM_TAG_LENGTH/8;
    }
}