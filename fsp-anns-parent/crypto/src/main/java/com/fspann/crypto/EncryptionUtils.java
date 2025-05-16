package com.fspann.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Low-level AES-GCM utilities for encrypting/decrypting raw byte arrays.
 */
public final class EncryptionUtils {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16; // bytes
    private static final SecureRandom RANDOM = new SecureRandom();

    private EncryptionUtils() {}

    public static byte[] generateIV() {
        byte[] iv = new byte[GCM_IV_LENGTH];
        RANDOM.nextBytes(iv);
        return iv;
    }

    public static byte[] encryptVector(double[] vector, byte[] iv, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv));

        byte[] plaintext = doubleArrayToBytes(vector);
        byte[] ciphertext = cipher.doFinal(plaintext);
        Arrays.fill(plaintext, (byte) 0);
        return ciphertext;
    }

    public static double[] decryptVector(byte[] ciphertext, byte[] iv, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv));

        byte[] plaintext = cipher.doFinal(ciphertext);
        try {
            return bytesToDoubleArray(plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }


    private static byte[] doubleArrayToBytes(double[] vector) {
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) buf.putDouble(d);
        return buf.array();
    }

    private static double[] bytesToDoubleArray(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) vector[i] = buf.getDouble();
        return vector;
    }
}
