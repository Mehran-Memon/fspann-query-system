package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Optional;

/**
 * Key/crypto helpers for testing and diagnostics.
 */
public class KeyUtils {

    /**
     * Attempts to decrypt an EncryptedPoint with a given key without any metadata services.
     * NOTE: Does NOT apply AAD. Intended only for diagnostics on legacy/plain data.
     */
    public static Optional<double[]> tryDecryptWithKeyOnly(EncryptedPoint pt, SecretKey key) {
        try {
            byte[] iv = pt.getIv();
            byte[] ciphertext = pt.getCiphertext();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(128, iv);
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, key, spec);
            byte[] plainBytes = cipher.doFinal(ciphertext);
            int dim = pt.getVectorLength();
            double[] vector = new double[dim];
            ByteBuffer buf = ByteBuffer.wrap(plainBytes);
            for (int i = 0; i < dim; i++) {
                vector[i] = buf.getDouble();
            }
            java.util.Arrays.fill(plainBytes, (byte) 0);
            return Optional.of(vector);
        } catch (GeneralSecurityException ex) {
            return Optional.empty();
        }
    }

    /** For testing: builds SecretKey from raw byte[]. */
    public static SecretKey fromBytes(byte[] rawKeyBytes) {
        if (rawKeyBytes == null || (rawKeyBytes.length != 16 && rawKeyBytes.length != 24 && rawKeyBytes.length != 32)) {
            throw new IllegalArgumentException("Invalid AES key length: " + (rawKeyBytes == null ? 0 : rawKeyBytes.length));
        }
        return new SecretKeySpec(rawKeyBytes, "AES");
    }
}
