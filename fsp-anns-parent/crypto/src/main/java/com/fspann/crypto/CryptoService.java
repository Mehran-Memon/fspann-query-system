package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;

import javax.crypto.SecretKey;

/**
 * CryptoService defines the contract for encryption and decryption
 * operations in the FSP-ANN framework using AES-GCM.
 */
public interface CryptoService {

    /**
     * Encrypts a vector and returns an EncryptedPoint with metadata.
     *
     * @param id Unique identifier for the vector.
     * @param vector The vector to encrypt.
     * @param key The AES-GCM key to use.
     * @return EncryptedPoint object.
     */
    EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key);

    /**
     * Decrypts an EncryptedPoint using the provided key.
     *
     * @param pt EncryptedPoint to decrypt.
     * @param key AES-GCM key used for decryption.
     * @return Decrypted vector.
     */
    double[] decryptFromPoint(EncryptedPoint pt, SecretKey key);

    /**
     * Encrypts a raw vector using the given key and IV.
     *
     * @param vector Vector to encrypt.
     * @param key AES-GCM key.
     * @param iv Initialization vector.
     * @return Ciphertext.
     */
    byte[] encrypt(double[] vector, SecretKey key, byte[] iv);

    /**
     * Decrypts a query ciphertext using the provided key and IV.
     *
     * @param encryptedQuery Ciphertext of the query.
     * @param iv Initialization vector used during encryption.
     * @param key AES-GCM key.
     * @return Decrypted vector.
     */
    double[] decryptQuery(byte[] encryptedQuery, byte[] iv, SecretKey key);

    /**
     * Re-encrypts a vector using a new key (key rotation support).
     *
     * @param pt The existing EncryptedPoint.
     * @param newKey New AES-GCM key to use.
     * @return Re-encrypted point.
     */
    EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey, byte[] newIv);

    /**
     * Generates a secure IV for AES-GCM encryption.
     *
     * @return Random IV.
     */
    byte[] generateIV();

    KeyLifeCycleService getKeyService();

    EncryptedPoint encrypt(String id, double[] vector);


}
