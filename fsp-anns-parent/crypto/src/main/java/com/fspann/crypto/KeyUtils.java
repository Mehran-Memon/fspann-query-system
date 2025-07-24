package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Optional;

public class KeyUtils {

    /**
     * Attempts to decrypt an EncryptedPoint with a given key without any metadata services.
     * @param pt EncryptedPoint to decrypt
     * @param key SecretKey to try
     * @return Optional<double[]> if successful, empty if failed
     */
    public static Optional<double[]> tryDecryptWithKeyOnly(EncryptedPoint pt, SecretKey key) {
        try {
            byte[] iv = pt.getIv();
            byte[] ciphertext = pt.getCiphertext();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(128, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            byte[] plainBytes = cipher.doFinal(ciphertext);
            int dim = pt.getVectorLength();
            double[] vector = new double[dim];
            ByteBuffer buf = ByteBuffer.wrap(plainBytes);
            for (int i = 0; i < dim; i++) {
                vector[i] = buf.getDouble();
            }
            return Optional.of(vector);
        } catch (GeneralSecurityException ex) {
            return Optional.empty();
        }
    }

    /**
     * For testing: builds SecretKey from raw byte[]
     */
    public static SecretKey fromBytes(byte[] rawKeyBytes) {
        if (rawKeyBytes == null || (rawKeyBytes.length != 16 && rawKeyBytes.length != 24 && rawKeyBytes.length != 32)) {
            throw new IllegalArgumentException("Invalid AES key length: " + (rawKeyBytes == null ? 0 : rawKeyBytes.length));
        }
        return new SecretKeySpec(rawKeyBytes, "AES");
    }
}

