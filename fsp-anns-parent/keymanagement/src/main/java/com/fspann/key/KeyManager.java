package com.fspann.key;

import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Core provider of master and session keys, with load/save support.
 * Uses HMAC-SHA256 for key derivation to ensure forward security.
 */
public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private static final String KEY_ALGORITHM = "AES";
    private static final String KDF_ALGORITHM = "HmacSHA256";
    private static final int KEY_SIZE = 256;

    private final String storagePath;
    private SecretKey masterKey;
    private final ConcurrentMap<Integer, SecretKey> sessionKeys = new ConcurrentHashMap<>();
    private final NavigableMap<Integer, SecretKey> shardKeys = new TreeMap<>();
    private volatile int currentVersion = 1;

    public KeyManager(String storagePath) throws IOException {
        this.storagePath = storagePath;
        Path p = Paths.get(storagePath);
        if (Files.exists(p)) {
            loadKeys(p);
        } else {
            initNewKeyStore(p);
        }
    }

    private void initNewKeyStore(Path path) throws IOException {
        masterKey = generateMasterKey();
        sessionKeys.put(currentVersion, deriveSessionKey(currentVersion));
        persist();
        logger.info("Initialized new key store (version {})", currentVersion);
    }

    private void loadKeys(Path path) throws IOException {
        try {
            KeyStoreBlob blob = PersistenceUtils.loadObject(
                    path.toString(),
                    new TypeReference<KeyStoreBlob>() {}
            );
            this.masterKey = blob.getMasterKey();
            this.sessionKeys.putAll(blob.getSessionKeys());
            this.currentVersion = blob.getSessionKeys().keySet().stream()
                    .max(Integer::compare)
                    .orElse(1);
            logger.info("Loaded key store up to version {}", currentVersion);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load keys, class not found: " + path, e);
        }
    }

    /** Rotate and return the new KeyVersion */
    public KeyVersion rotateKey() {
        currentVersion++;
        SecretKey newKey = deriveSessionKey(currentVersion);
        sessionKeys.put(currentVersion, newKey);
        try { persist(); } catch (IOException e) {
            logger.error("Failed to persist rotated key store", e);
        }
        return new KeyVersion(currentVersion, newKey);
    }

    public KeyVersion getCurrentVersion() {
        SecretKey key = sessionKeys.get(currentVersion);
        return new KeyVersion(currentVersion, key);
    }

    public SecretKey getSessionKey(int version) {
        return sessionKeys.get(version);
    }

    /**
     * Derives a session key from the masterKey using HMAC-SHA256 KDF.
     */
    private SecretKey deriveSessionKey(int version) {
        try {
            Mac mac = Mac.getInstance(KDF_ALGORITHM);
            mac.init(masterKey);
            byte[] derived = mac.doFinal(("session-" + version).getBytes());
            byte[] keyBytes = new byte[KEY_SIZE / 8];
            System.arraycopy(derived, 0, keyBytes, 0, keyBytes.length);
            return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
        } catch (Exception e) {
            throw new RuntimeException("Session key derivation failed", e);
        }
    }

    private SecretKey generateMasterKey() {
        try {
            KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);
            kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
            return kg.generateKey();
        } catch (Exception e) {
            throw new SecurityException("Master key generation failed", e);
        }
    }

    private void persist() throws IOException {
        KeyStoreBlob blob = new KeyStoreBlob(masterKey, sessionKeys);
        PersistenceUtils.saveObject(blob, storagePath);
    }

    // DTO for serializing keys
    private static class KeyStoreBlob implements java.io.Serializable {
        private final SecretKey masterKey;
        private final ConcurrentMap<Integer, SecretKey> sessionKeys;
        public KeyStoreBlob(SecretKey masterKey, ConcurrentMap<Integer, SecretKey> sessionKeys) {
            this.masterKey = masterKey;
            this.sessionKeys = sessionKeys;
        }
        public SecretKey getMasterKey() { return masterKey; }
        public ConcurrentMap<Integer, SecretKey> getSessionKeys() { return sessionKeys; }
    }
}