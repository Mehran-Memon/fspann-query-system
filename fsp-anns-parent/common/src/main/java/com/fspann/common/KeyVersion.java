package com.fspann.common;

import javax.crypto.SecretKey;
import java.io.Serializable;

public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int version;
    private final SecretKey secretkey;

    public KeyVersion(int version, SecretKey key) {
        this.version = version;
        this.secretkey = key;
    }

    public int getVersion() { return version; }
    public SecretKey getSecretKey() { return secretkey; }
}
