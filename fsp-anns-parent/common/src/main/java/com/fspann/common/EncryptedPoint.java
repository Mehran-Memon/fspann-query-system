package com.fspann.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class EncryptedPoint implements Serializable {
    private final String id;
    private final int shardId;
    private final byte[] iv;
    private final byte[] ciphertext;
    private final int version;
    private final int vectorLength;

    public EncryptedPoint(String id, int shardId, byte[] iv, byte[] ciphertext, int version, int vectorLength) {
        this.id = Objects.requireNonNull(id, "ID must not be null");
        this.shardId = shardId;
        this.iv = iv != null ? iv.clone() : null;
        this.ciphertext = ciphertext != null ? ciphertext.clone() : null;
        this.version = version;
        this.vectorLength = vectorLength;
        if (iv == null || ciphertext == null) {
            throw new NullPointerException("IV or ciphertext must not be null");
        }
    }

    public String getId() { return id; }
    public int getShardId() { return shardId; }
    public byte[] getIv() { return iv != null ? iv.clone() : null; }
    public byte[] getCiphertext() { return ciphertext != null ? ciphertext.clone() : null; }
    public int getVersion() { return version; }
    public int getVectorLength() { return vectorLength; }

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
                Arrays.equals(ciphertext, that.ciphertext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, shardId, version, vectorLength, Arrays.hashCode(iv), Arrays.hashCode(ciphertext));
    }
}