package com.fspann.encryption;

import com.fspann.index.BucketConstructor;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;

public class EncryptionUtils {

    private static final String ALGORITHM = "AES/GCM/NoPadding"; // AES with GCM for authenticated encryption
    private static final int GCM_TAG_LENGTH = 128; // Authentication tag length for GCM (128 bits)

    // Method to generate a random IV for AES GCM mode (12-byte for GCM)
    private static byte[] generateIV() {
        byte[] iv = new byte[12]; // GCM requires a 12-byte IV
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    // Method to encrypt the vector using AES-GCM and return the IV + encrypted data
    public static byte[] encryptVector(double[] vector, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        byte[] iv = generateIV();  // Generate a new IV for each encryption
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv); // Set GCM parameters (IV and tag length)
        cipher.init(Cipher.ENCRYPT_MODE, key, spec);

        // Convert the vector into a byte array
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double v : vector) {
            buffer.putDouble(v);
        }

        // Perform the encryption
        byte[] cipherText = cipher.doFinal(buffer.array());

        // Combine IV and ciphertext
        byte[] encryptedData = new byte[iv.length + cipherText.length];
        System.arraycopy(iv, 0, encryptedData, 0, iv.length);
        System.arraycopy(cipherText, 0, encryptedData, iv.length, cipherText.length);

        return encryptedData; // Return the concatenated IV + encrypted data
    }

    // Method to decrypt the encrypted data using AES-GCM
    public static double[] decryptVector(byte[] encryptedData, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);

        // Extract IV from the encrypted data (first 12 bytes are the IV)
        byte[] iv = new byte[12];
        byte[] cipherText = new byte[encryptedData.length - iv.length];

        System.arraycopy(encryptedData, 0, iv, 0, iv.length); // Extract the IV
        System.arraycopy(encryptedData, iv.length, cipherText, 0, cipherText.length); // Extract the cipherText

        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.DECRYPT_MODE, key, spec);

        // Perform the decryption
        byte[] decryptedData = cipher.doFinal(cipherText);

        // Convert the decrypted byte array back into a double array (Vector)
        return BucketConstructor.byteToDoubleArray(decryptedData);
    }

    // Method to perform re-encryption with a new key after key rotation
    public static byte[] reEncryptData(byte[] encryptedData, SecretKey oldKey, SecretKey newKey) throws Exception {
        // Step 1: Decrypt the data using the old key
        double[] decryptedData = decryptVector(encryptedData, oldKey);

        // Step 2: Re-encrypt the decrypted data with the new key
        return encryptVector(decryptedData, newKey);
    }
}
