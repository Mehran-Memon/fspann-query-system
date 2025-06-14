package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import javax.crypto.SecretKey;

public interface CryptoService {
    EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key);
    double[] decryptFromPoint(EncryptedPoint pt, SecretKey key);
    byte[] encrypt(double[] vector, SecretKey key, byte[] iv);
    double[] decryptQuery(byte[] encryptedQuery, byte[] iv, SecretKey key);
    EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey);
    byte[] generateIV();
}