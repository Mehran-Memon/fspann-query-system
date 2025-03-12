package com.fspann.query;

public class EncryptedPoint {
    private byte[] ciphertext;  // The encrypted coordinate data
    private String bucketId;    // ID or label for the bucket

    // Optionally store a unique ID, or any other metadata needed
    private String pointId;

    public EncryptedPoint(byte[] ciphertext, String bucketId, String pointId) {
        this.ciphertext = ciphertext;
        this.bucketId = bucketId;
        this.pointId = pointId;
    }

    public byte[] getCiphertext() {
        return ciphertext;
    }

    public String getBucketId() {
        return bucketId;
    }

    public String getPointId() {
        return pointId;
    }
}
