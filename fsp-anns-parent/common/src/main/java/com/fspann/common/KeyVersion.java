package com.fspann.common;

import javax.crypto.SecretKey;
import java.io.Serializable;
import java.util.Arrays;

public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int version;
    private final SecretKey key;

    public KeyVersion(int version, SecretKey key) {
        this.version = version;
        this.key = key;
    }
    public int getVersion() { return version; }
    public SecretKey getKey() { return key; }
    @Override
    public String toString() {
        return "KeyVersion{version=" + version +
                ", keyHash=" + Arrays.hashCode(key.getEncoded()) + "}";
    }
}
