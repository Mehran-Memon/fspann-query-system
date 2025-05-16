// in crypto module
package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import javax.crypto.SecretKey;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import java.util.Arrays;

public class AesGcmCryptoService implements CryptoService {
    @Override
    public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
        try {
            byte[] iv = EncryptionUtils.generateIV();
            byte[] ct = EncryptionUtils.encryptVector(vector, iv, key);
            // shardId must come from the index layer, so use 0 or a placeholder here:
            return new EncryptedPoint(id, /* shardId */ 0, iv, ct);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double[] decryptFromPoint(EncryptedPoint p, SecretKey key) {
        try {
            return EncryptionUtils.decryptVector(p.getCiphertext(), p.getIv(), key);
        } catch (Exception e) {
            throw new RuntimeException("Decryption failed for point "+p.getId(), e);
        }
    }

}
