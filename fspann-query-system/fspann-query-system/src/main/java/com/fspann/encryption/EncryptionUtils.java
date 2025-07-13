package com.fspann.encryption;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;

public class EncryptionUtils {
    public static byte[] encryptVector(double[] vector, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double v : vector) {
            buffer.putDouble(v);
        }
        return cipher.doFinal(buffer.array());
    }

    public static double[] decryptVector(byte[] encryptedData, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decryptedData = cipher.doFinal(encryptedData);
        ByteBuffer buffer = ByteBuffer.wrap(decryptedData);
        double[] vector = new double[decryptedData.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();
        }
        return vector;
    }

    public static byte[] reEncrypt(byte[] encryptedData, SecretKey oldKey, SecretKey newKey) throws Exception {
        double[] decryptedData = decryptVector(encryptedData, oldKey);
        return encryptVector(decryptedData, newKey);
    }
}