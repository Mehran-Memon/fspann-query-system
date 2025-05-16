package com.fspann.loader;

/**
 * Immutable DTO for an encrypted vector payload.
 */
public class EncryptedVector {
    private final byte[] encryptedData;
    private final String id;

    public EncryptedVector(byte[] encryptedData, String id) {
        this.encryptedData = encryptedData.clone();
        this.id = id;
    }

    public byte[] getEncryptedData() {
        return encryptedData.clone();
    }

    public String getId() {
        return id;
    }
}
