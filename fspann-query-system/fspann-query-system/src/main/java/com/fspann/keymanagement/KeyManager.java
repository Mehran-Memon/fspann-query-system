package com.fspann.keymanagement;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyManager {
    private SecretKey masterKey;
    private Map<String, SecretKey> sessionKeys;
    private final AtomicInteger timeEpoch;
    private final int rotationInterval;

    public KeyManager(int rotationInterval) {
        this.sessionKeys = new HashMap<>();
        this.timeEpoch = new AtomicInteger(0);
        this.rotationInterval = rotationInterval;
        initializeMasterKey();
    }

    private void initializeMasterKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            this.masterKey = keyGen.generateKey();
            generateSessionKey("epoch_0");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize master key: " + e.getMessage());
        }
    }

    public SecretKey generateSessionKey(String context) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        SecretKey sessionKey = keyGen.generateKey();
        sessionKeys.put(context, sessionKey);
        return sessionKey;
    }

    public SecretKey getSessionKey(String context) {
        return sessionKeys.get(context);
    }

    public SecretKey getCurrentKey() {
        return getSessionKey("epoch_" + timeEpoch.get());
    }

    public SecretKey getPreviousKey() {
        int prevEpoch = timeEpoch.get() - 1;
        return prevEpoch >= 0 ? getSessionKey("epoch_" + prevEpoch) : null;
    }

    public SecretKey rotateKey(String context, byte[] encryptedData) throws Exception {
        int currentEpoch = timeEpoch.getAndIncrement();
        SecretKey oldKey = getSessionKey(context);
        if (oldKey == null) {
            throw new IllegalArgumentException("No key found for context: " + context);
        }

        SecretKey newKey = generateSessionKey("epoch_" + currentEpoch);
        if (encryptedData != null) {
            byte[] reEncryptedData = EncryptionUtils.reEncrypt(encryptedData, oldKey, newKey);
        }
        return newKey;
    }

    public boolean needsRotation(int operationCount) {
        return operationCount >= rotationInterval;
    }

    public void rotateAllKeys(List<String> contexts, Map<String, byte[]> encryptedDataMap) throws Exception {
        for (String context : contexts) {
            byte[] encryptedData = encryptedDataMap.get(context);
            rotateKey(context, encryptedData);
        }
    }

    public int getTimeEpoch() {
        return timeEpoch.get();
    }
}