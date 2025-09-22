package com.fspann.common;

import javax.crypto.SecretKey;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a versioned AES key used for encryption and forward security enforcement.
 */
public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int version;
    private final SecretKey key;

    public KeyVersion(int version, SecretKey key) {
        if (version < 0) throw new IllegalArgumentException("Version must be non-negative");
        this.version = version;
        this.key = Objects.requireNonNull(key, "SecretKey must not be null");
    }

    public int getVersion() {
        return version;
    }

    public SecretKey getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "KeyVersion{version=" + version +
                ", keyHash=" + Arrays.hashCode(key.getEncoded()) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyVersion)) return false;
        KeyVersion that = (KeyVersion) o;
        return version == that.version &&
                Arrays.equals(this.key.getEncoded(), that.key.getEncoded());
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, Arrays.hashCode(key.getEncoded()));
    }
}
