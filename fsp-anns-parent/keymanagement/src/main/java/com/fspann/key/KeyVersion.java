package com.fspann.key;

import javax.crypto.SecretKey;
import java.io.Serializable;

public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int version;
    private final SecretKey key;

    public KeyVersion(int version, SecretKey key) {
        this.version = version;
        this.key = key;
    }

    public int getVersion() { return version; }
    public SecretKey getSecretKey() { return key; }
}
