package com.fspann.data;

public class EncryptedVector {
    private byte[] encryptedData;
    private String id;

    public EncryptedVector(byte[] encryptedData, String id) {
        this.encryptedData = encryptedData;
        this.id = id;
    }

    public byte[] getEncryptedData() {
        return encryptedData;
    }

    public String getId() {
        return id;
    }
}
