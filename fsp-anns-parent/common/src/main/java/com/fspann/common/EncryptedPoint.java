package com.fspann.common;


import java.io.Serializable;

/**
 * Represents an encrypted data point with its identifier, shard (bucket) ID, IV, ciphertext, and dimension.
 * This is a pure DTO and does not contain any crypto logic.
 */
public class EncryptedPoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final int shardId;
    private final byte[] iv;
    private final byte[] ciphertext;
    private final int version; // Added version field
    private final int vectorLength; // New field for dimension tracking

    public EncryptedPoint(String id, int shardId, byte[] iv, byte[] ciphertext, int version, int vectorLength) {
        if (id == null || iv == null || ciphertext == null) {
            throw new NullPointerException("id, iv, and ciphertext must not be null");
        }
        this.id = id;
        this.shardId = shardId;
        this.iv = iv.clone();
        this.ciphertext = ciphertext.clone();
        this.version = version;
        this.vectorLength = vectorLength;
    }

    public String getId() { return id; }
    public int getShardId() { return shardId; }
    public byte[] getIv() { return iv.clone(); }
    public byte[] getCiphertext() { return ciphertext.clone(); }
    public int getVersion() { return version; }
    public int getVectorLength() { return vectorLength; } // New getter
}
