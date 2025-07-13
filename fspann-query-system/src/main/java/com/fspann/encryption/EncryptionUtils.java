package com.fspann.encryption;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.security.SecureRandom;

public class EncryptionUtils {

    public static byte[] encrypt(byte[] plaintext, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        byte[] iv = new byte[cipher.getBlockSize()];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(iv);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
        byte[] ciphertext = cipher.doFinal(plaintext);

        // You might prepend the IV to the ciphertext, or store separately
        byte[] combined = new byte[iv.length + ciphertext.length];
        System.arraycopy(iv, 0, combined, 0, iv.length);
        System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);
        return combined;
    }

    public static byte[] decrypt(byte[] combined, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        int blockSize = cipher.getBlockSize();

        byte[] iv = new byte[blockSize];
        byte[] ciphertext = new byte[combined.length - blockSize];

        System.arraycopy(combined, 0, iv, 0, blockSize);
        System.arraycopy(combined, blockSize, ciphertext, 0, ciphertext.length);

        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
        return cipher.doFinal(ciphertext);
    }
}
