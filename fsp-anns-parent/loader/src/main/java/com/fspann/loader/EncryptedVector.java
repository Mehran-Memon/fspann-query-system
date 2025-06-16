package com.fspann.loader;

/**
 * Immutable DTO for an encrypted vector payload.
 */
public class EncryptedVector {
    private final byte[] encryptedData;
    private final String id;

    public EncryptedVector(byte[] encryptedData, String id) {
        if (encryptedData == null || encryptedData.length == 0)
            throw new IllegalArgumentException("encryptedData cannot be null or empty");
        this.encryptedData = encryptedData.clone();
        this.id = (id != null) ? id : "";
    }

    public byte[] getEncryptedData() {
        return encryptedData.clone();
    }

    public String getId() {
        return id;
    }
}
