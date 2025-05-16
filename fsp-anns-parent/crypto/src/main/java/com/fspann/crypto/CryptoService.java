// in crypto module
package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import javax.crypto.SecretKey;

public interface CryptoService {
    EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key);
    double[]      decryptFromPoint(EncryptedPoint point,    SecretKey key);
}
