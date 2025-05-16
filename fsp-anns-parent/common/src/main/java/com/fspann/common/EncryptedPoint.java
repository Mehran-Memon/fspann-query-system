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

    public EncryptedPoint(String id, int shardId, byte[] iv, byte[] ciphertext) {
        this.id = Objects.requireNonNull(id, "id");
        this.shardId = shardId;
        this.iv = Objects.requireNonNull(iv, "iv").clone();
        this.ciphertext = Objects.requireNonNull(ciphertext, "ciphertext").clone();
    }

    public String getId() {
        return id;
    }

    /**
     * @return the bucket (shard) this point belongs to
     */
    public int getShardId() {
        return shardId;
    }

    public byte[] getIv() {
        return iv.clone();
    }

    public byte[] getCiphertext() {
        return ciphertext.clone();
    }

    @Override
    public String toString() {
        return "EncryptedPoint{" +
                "id='" + id + '\'' +
                ", shardId=" + shardId +
                ", iv=" + Arrays.toString(iv) +
                ", ciphertext=" + Arrays.toString(ciphertext) +
                '}';
    }
}
