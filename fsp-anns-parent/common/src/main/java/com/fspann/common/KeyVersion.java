package com.fspann.common;

import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * KeyVersion: Represents a single version of the key.
 *
 * version    = version number (e.g., 1, 2, 3, ...)
 * key        = actual SecretKey
 * createdAt  = timestamp when key was created
 * rotatedAt  = timestamp when key was rotated (0 if current)
 */
public final class KeyVersion {
    private final int version;
    private final SecretKey key;
    private final long createdAt;
    private final long rotatedAt;

    /**
     * Full constructor with timestamps.
     */
    public KeyVersion(int version, SecretKey key, long createdAt, long rotatedAt) {
        this.version = version;
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.createdAt = createdAt;
        this.rotatedAt = rotatedAt;
    }

    /**
     * Convenience constructor (createdAt = now, rotatedAt = 0).
     */
    public KeyVersion(int version, SecretKey key) {
        this(version, key, System.currentTimeMillis(), 0L);
    }

    public int getVersion() {
        return version;
    }

    public SecretKey getKey() {
        return key;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getRotatedAt() {
        return rotatedAt;
    }

    @Override
    public String toString() {
        return String.format("KeyVersion{v=%d, created=%d, rotated=%d}",
                version, createdAt, rotatedAt);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof KeyVersion that)) return false;
        return version == that.version && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, key);
    }
}