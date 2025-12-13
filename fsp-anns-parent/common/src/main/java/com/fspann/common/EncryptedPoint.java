package com.fspann.common;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * EncryptedPoint: An encrypted vector with metadata.
 *
 * Implements Serializable for persistence via PersistenceUtils.
 */
public class EncryptedPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final int version;
    private final byte[] iv;
    private final byte[] ciphertext;
    private final int keyVersion;
    private final int dimension;
    private final int shardId;  // <-- NEW: for data sharding
    private final List<Integer> buckets;  // <-- NEW: for LSH bucket IDs
    private final List<String> metadata;

    /**
     * Full constructor with all fields.
     */
    public EncryptedPoint(
            String id,
            int version,
            byte[] iv,
            byte[] ciphertext,
            int keyVersion,
            int dimension,
            int shardId,
            List<Integer> buckets,
            List<String> metadata
    ) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.version = version;
        this.iv = Objects.requireNonNull(iv, "iv cannot be null");
        this.ciphertext = Objects.requireNonNull(ciphertext, "ciphertext cannot be null");
        this.keyVersion = keyVersion;
        this.dimension = dimension;
        this.shardId = shardId;
        this.buckets = (buckets != null) ? buckets : List.of();
        this.metadata = (metadata != null) ? metadata : List.of();
    }

    /**
     * Convenience constructor (backward compatible - default shardId=0, empty buckets).
     */
    public EncryptedPoint(
            String id,
            int version,
            byte[] iv,
            byte[] ciphertext,
            int keyVersion,
            int dimension,
            List<String> metadata
    ) {
        this(id, version, iv, ciphertext, keyVersion, dimension, 0, List.of(), metadata);
    }

    // ==================== GETTERS ====================

    public String getId() { return id; }
    public int getVersion() { return version; }
    public byte[] getIv() { return iv; }
    public byte[] getCiphertext() { return ciphertext; }
    public int getKeyVersion() { return keyVersion; }
    public int getDimension() { return dimension; }
    public int getVectorLength() { return dimension; }  // Alias
    public int getShardId() { return shardId; }  // <-- NEW
    public List<Integer> getBuckets() { return buckets; }  // <-- NEW
    public List<String> getMetadata() { return metadata; }

    @Override
    public String toString() {
        return String.format(
                "EncryptedPoint{id=%s, v=%d, dim=%d, shard=%d, buckets=%d, ctLen=%d}",
                id, version, dimension, shardId, buckets.size(), ciphertext.length
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof EncryptedPoint that)) return false;
        return Objects.equals(id, that.id)
                && version == that.version
                && dimension == that.dimension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, dimension);
    }
}