//package com.fspann.common;
//
//import javax.crypto.SecretKey;
//import java.io.Serializable;
//
//public class KeyVersion implements Serializable {
//    private static final long serialVersionUID = 1L;
//        private final int version;
//        private final SecretKey secretKey;
//        private final byte[] iv;
//        private final byte[] encryptedQuery;
//
//        public KeyVersion(int version, SecretKey secretKey, byte[] iv, byte[] encryptedQuery) {
//            this.version = version;
//            this.secretKey = secretKey;
//            this.iv = iv;
//            this.encryptedQuery = encryptedQuery;
//        }
//
//        public byte[] getIv()                 { return iv; }
//        public byte[] getEncryptedQuery()     { return encryptedQuery; }
//    public int getVersion() { return version; }
//    public SecretKey getSecretKey() { return secretKey; }
//}
//
//
package com.fspann.common;

import javax.crypto.SecretKey;
import java.io.Serializable;

public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L; // Required for Serializable
    private final int version;
    private final SecretKey key;

    public KeyVersion(int version, SecretKey key) {
        this.version = version;
        this.key = key;
    }

    public int getVersion() { return version; }
    public SecretKey getKey() { return key; }
}