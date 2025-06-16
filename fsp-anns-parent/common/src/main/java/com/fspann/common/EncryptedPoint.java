package com.fspann.common;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents an encrypted data point with its identifier, shard (bucket) ID, IV, and ciphertext.
 * This is a pure DTO and does not contain any crypto logic.
 */
public class EncryptedPoint {
    private final String id;
    private final int shardId;
    private final byte[] iv;
    private final byte[] ciphertext;
    private final int version; // Added version field

    public EncryptedPoint(String id, int shardId, byte[] iv, byte[] ciphertext, int version) {
        if (id == null || iv == null || ciphertext == null) {
            throw new NullPointerException("id, iv, and ciphertext must not be null");
        }
        this.id = id;
        this.shardId = shardId;
        this.iv = iv.clone();
        this.ciphertext = ciphertext.clone();
        this.version = version;
    }

    public String getId() { return id; }
    public int getShardId() { return shardId; }
    public byte[] getIv() { return iv.clone(); }
    public byte[] getCiphertext() { return ciphertext.clone(); }
    public int getVersion() { return version; } // Added getter
}