package com.fspann.encryption;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;

/**
 * Utility class for encryption and decryption using AES/CBC/PKCS5Padding.
 * Supports forward security with key rotation and high-dimensional vector encryption.
 */
public class EncryptionUtils {

    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    private static final int IV_LENGTH = 16; // AES block size in bytes

    /**
     * Encrypts a byte array using the provided key.
     * Prepends the IV to the ciphertext for self-contained storage.
     */
    public static byte[] encrypt(byte[] plaintext, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        byte[] iv = new byte[IV_LENGTH];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(iv);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
        byte[] ciphertext = cipher.doFinal(plaintext);

        byte[] combined = new byte[IV_LENGTH + ciphertext.length];
        System.arraycopy(iv, 0, combined, 0, IV_LENGTH);
        System.arraycopy(ciphertext, 0, combined, IV_LENGTH, ciphertext.length);
        return combined;
    }

    /**
     * Decrypts a combined IV+ciphertext byte array using the provided key.
     */
    public static byte[] decrypt(byte[] combined, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        byte[] iv = new byte[IV_LENGTH];
        byte[] ciphertext = new byte[combined.length - IV_LENGTH];

        System.arraycopy(combined, 0, iv, 0, IV_LENGTH);
        System.arraycopy(combined, IV_LENGTH, ciphertext, 0, ciphertext.length);

        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
        return cipher.doFinal(ciphertext);
    }

    /**
     * Encrypts a high-dimensional vector (double[]) into a byte array.
     */
    public static byte[] encryptVector(double[] vector, SecretKey key) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double value : vector) {
            buffer.putDouble(value);
        }
        return encrypt(buffer.array(), key);
    }

    /**
     * Decrypts a byte array back into a high-dimensional vector (double[]).
     */
    public static double[] decryptVector(byte[] encryptedVector, SecretKey key) throws Exception {
        byte[] decryptedBytes = decrypt(encryptedVector, key);
        ByteBuffer buffer = ByteBuffer.wrap(decryptedBytes);
        double[] vector = new double[decryptedBytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();
        }
        return vector;
    }

    /**
     * Re-encrypts data with a new key for forward security.
     * Useful during key rotation to maintain forward secrecy.
     */
    public static byte[] reEncrypt(byte[] encryptedData, SecretKey oldKey, SecretKey newKey) throws Exception {
        byte[] decrypted = decrypt(encryptedData, oldKey);
        return encrypt(decrypted, newKey);
    }
}