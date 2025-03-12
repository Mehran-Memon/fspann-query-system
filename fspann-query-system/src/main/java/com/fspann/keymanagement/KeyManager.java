package com.fspann.keymanagement;

import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class KeyManager {
    private SecretKey masterKey;
    private Map<String, SecretKey> sessionKeys;  // e.g., keyed by bucket ID or by time epoch

    public KeyManager() {
        this.sessionKeys = new HashMap<>();
    }

    public void generateMasterKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        this.masterKey = keyGen.generateKey();
    }

    public SecretKey getMasterKey() {
        return masterKey;
    }

    public SecretKey generateSessionKey(String context) throws Exception {
        // context could be e.g. "bucketId" or "timestamp"
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        SecretKey sessionKey = keyGen.generateKey();
        sessionKeys.put(context, sessionKey);
        return sessionKey;
    }

    public SecretKey getSessionKey(String context) {
        return sessionKeys.get(context);
    }

    // For forward security, you might do something like remove old sessionKeys,
    // or re-encrypt data from old key to new key using proxy re-encryption, etc.
    public void rotateKey(String context, SecretKey newKey) {
        sessionKeys.put(context, newKey);
    }
}
