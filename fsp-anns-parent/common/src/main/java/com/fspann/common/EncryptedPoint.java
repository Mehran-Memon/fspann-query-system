package com.fspann.common;

import java.io.Serializable;
import java.util.*;

public class EncryptedPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final int shardId;
    private final byte[] iv;
    private final byte[] ciphertext;
    private final int version;
    private final int vectorLength;
    private final List<Integer> buckets;

    public EncryptedPoint(String id, int shardId, byte[] iv, byte[] ciphertext, int version, int vectorLength, List<Integer> buckets) {
        this.id = Objects.requireNonNull(id, "ID must not be null");
        this.shardId = shardId;
        this.iv = Objects.requireNonNull(iv, "IV must not be null").clone();
        this.ciphertext = Objects.requireNonNull(ciphertext, "Ciphertext must not be null").clone();
        if (version < 0) throw new IllegalArgumentException("Version must be non-negative");
        if (vectorLength <= 0) throw new IllegalArgumentException("Vector length must be positive");
        this.version = version;
        this.vectorLength = vectorLength;
        this.buckets = buckets != null ? Collections.unmodifiableList(new ArrayList<>(buckets)) : Collections.emptyList();
    }

    public String getId() { return id; }
    public int getShardId() { return shardId; }
    public byte[] getIv() { return iv.clone(); }
    public byte[] getCiphertext() { return ciphertext.clone(); }
    public int getVersion() { return version; }
    public int getVectorLength() { return vectorLength; }
    public List<Integer> getBuckets() { return new ArrayList<>(buckets); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EncryptedPoint)) return false;
        EncryptedPoint that = (EncryptedPoint) o;
        return shardId == that.shardId &&
                version == that.version &&
                vectorLength == that.vectorLength &&
                Objects.equals(id, that.id) &&
                Arrays.equals(iv, that.iv) &&
                Arrays.equals(ciphertext, that.ciphertext) &&
                Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, shardId, Arrays.hashCode(iv), Arrays.hashCode(ciphertext), version, vectorLength, buckets);
    }

    @Override
    public String toString() {
        return String.format("EncryptedPoint[id=%s, shard=%d, version=%d, dim=%d]", id, shardId, version, vectorLength);
    }
}
