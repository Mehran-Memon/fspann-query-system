package com.fspann.index;

import com.fspann.keymanagement.KeyManager;
import com.fspann.encryption.EncryptionUtils;

import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class DimensionContext {
    private final ANN ann;
    private final EvenLSH lsh;
    private final SecureLSHIndex index;
    private final List<double[]> baseVectors;
    private byte[] encryptedData;  // This holds the encrypted vector data
    private String id;
    private byte[] iv;

    public DimensionContext(int dimensions, int numIntervals, int numHashTables, KeyManager keyManager, List<double[]> baseVectors) {
        this.lsh = new EvenLSH(dimensions, numIntervals);
        this.ann = new ANN(dimensions, numIntervals, keyManager, baseVectors);
        this.encryptedData = encryptedData;
        this.id = id;

        // Initialize SecureLSHIndex with shards = numHashTables
        this.index = new SecureLSHIndex(
                numHashTables,            // number of hash tables
                numHashTables,            // number of shards
                keyManager.getCurrentKey(),
                baseVectors
        );
        this.baseVectors = baseVectors;
    }

    public byte[] getEncryptedData() {
        return encryptedData;
    }

    public void setEncryptedData(byte[] encryptedData) {
        this.encryptedData = encryptedData;
    }

    public ANN getAnn() {
        return ann;
    }

    public EvenLSH getLsh() {
        return lsh;
    }

    public SecureLSHIndex getIndex() {
        return index;
    }

    public List<double[]> getBaseVectors() {
        return baseVectors;
    }

    public String getId() {
        return id;
    }

    // Implementing the reEncrypt method
    public void reEncrypt(KeyManager keyManager, SecretKey previousKey) throws Exception {
        Objects.requireNonNull(keyManager, "KeyManager cannot be null");
        Objects.requireNonNull(previousKey, "Previous key cannot be null");

        // Step 1: Decrypt using previous key and stored IV
        double[] decryptedVector = EncryptionUtils.decryptVector(
                this.encryptedData,  // Encrypted data
                this.iv,
                previousKey          // Previous key
        );

        if (decryptedVector == null) {
            throw new Exception("Decryption failed with the previous key.");
        }

        // Step 2: Generate new IV for re-encryption
        byte[] newIv = EncryptionUtils.generateIV();
        SecretKey currentKey = keyManager.getCurrentKey();

        if (currentKey == null) {
            throw new IllegalStateException("Current key cannot be null for re-encryption.");
        }

        // Step 3: Re-encrypt with new key (using generated IV)
        byte[] reEncryptedData = EncryptionUtils.encryptVector(
                decryptedVector,   // Decrypted vector
                iv,
                currentKey         // New encryption key
        );

        // Step 4: Update both encrypted data and IV
        this.encryptedData = reEncryptedData;  // Store re-encrypted data
        this.iv = newIv;                       // Store the new IV

        // Step 5: Clean up sensitive data
        Arrays.fill(decryptedVector, 0.0);  // Zero out sensitive data

        System.out.println("Successfully re-encrypted data for context: " + this.id);
    }
}