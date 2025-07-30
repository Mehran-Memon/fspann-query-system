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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Core provider of master and session keys, with load/save support.
 * Uses HMAC-SHA256 for key derivation to ensure forward security.
 */
public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);
    private static final String KEY_ALGORITHM = "AES";
    private static final String KDF_ALGORITHM = "HmacSHA256";
    private static final int KEY_SIZE = 256;
    private final ConcurrentMap<Integer, SecretKey> derivedKeys = new ConcurrentHashMap<>();
    private final String storagePath;
    private SecretKey masterKey;
    private final ConcurrentMap<Integer, SecretKey> sessionKeys = new ConcurrentHashMap<>();
    private final NavigableMap<Integer, SecretKey> shardKeys = new TreeMap<>();
    private volatile int currentVersion = 1;

    public KeyManager(String storagePath) throws IOException {
        if (storagePath == null) {
            throw new IllegalArgumentException("Key path must not be null");
        }
        this.storagePath = Paths.get(storagePath).toAbsolutePath().normalize().toString();
        Path p = Paths.get(this.storagePath);

        if (Files.exists(p)) {
            loadKeys(p);
        } else {
            initNewKeyStore(p);
        }
    }

    private synchronized void initNewKeyStore(Path path) throws IOException {
        try {
            masterKey = generateMasterKey();
            sessionKeys.put(currentVersion, deriveSessionKey(currentVersion));
            persist();
            logger.debug("Initialized new key store (version {})", currentVersion);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            logger.error("Failed to initialize key store", e);
            throw new IOException("Failed to initialize key store", e);
        }
    }

    private synchronized void loadKeys(Path path) throws IOException {
        try {
            KeyStoreBlob blob = PersistenceUtils.loadObject(path.toString(), storagePath, KeyStoreBlob.class);
            this.masterKey = blob.getMasterKey();
            this.sessionKeys.putAll(blob.getSessionKeys());
            this.currentVersion = blob.getSessionKeys().keySet().stream()
                    .max(Integer::compare)
                    .orElse(1);
            logger.debug("Loaded key store up to version {}", currentVersion);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load keys, class not found: " + path, e);
        }
    }

    public SecretKey getSessionKey(int version) {
        logger.debug("Fetching key for version: {}", version);
        return sessionKeys.get(version);
    }

    public KeyVersion getCurrentVersion() {
        return new KeyVersion(currentVersion, sessionKeys.get(currentVersion));
    }

    public KeyVersion getPreviousVersion() {
        Integer previousVersion = sessionKeys.keySet().stream()
                .filter(v -> v < currentVersion)
                .max(Integer::compare)
                .orElse(null);
        if (previousVersion == null) return null;
        return new KeyVersion(previousVersion, sessionKeys.get(previousVersion));
    }

    public KeyVersion rotateKey() {
        currentVersion++;
        try {
            SecretKey newKey = deriveSessionKey(currentVersion);
            sessionKeys.put(currentVersion, newKey);
            persist();
            return new KeyVersion(currentVersion, newKey);
        } catch (NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            logger.error("Key rotation failed for version {}: {}", currentVersion, e.getMessage(), e);
            throw new RuntimeException("Key rotation failed", e);
        }
    }

    /**
     * Derives a session key from the masterKey using HMAC-SHA256 KDF.
     */
    public SecretKey deriveSessionKey(int version) throws NoSuchAlgorithmException, InvalidKeyException {
        return derivedKeys.computeIfAbsent(version, v -> {
            byte[] salt = ByteBuffer.allocate(4).putInt(v).array();
            Mac mac = null;
            try {
                mac = Mac.getInstance(KDF_ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            try {
                mac.init(masterKey);
            } catch (InvalidKeyException e) {
                throw new RuntimeException(e);
            }
            byte[] derived = mac.doFinal(salt);
            byte[] keyBytes = new byte[KEY_SIZE / 8];
            System.arraycopy(derived, 0, keyBytes, 0, keyBytes.length);
            return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
        });
    }

    private SecretKey generateMasterKey() throws NoSuchAlgorithmException {
        KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);
        kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
        return kg.generateKey();
    }

    private synchronized void persist() throws IOException {
        KeyStoreBlob blob = new KeyStoreBlob(masterKey, sessionKeys);
        PersistenceUtils.saveObject(blob, storagePath, storagePath);
    }

    public void init() {
        // Ensure version 1 exists
        if (!sessionKeys.containsKey(1)) {
            try {
                sessionKeys.put(1, deriveSessionKey(1));
                persist();
                logger.debug("Initialized session key version 1 manually via init()");
            } catch (NoSuchAlgorithmException | InvalidKeyException | IOException e) {
                throw new RuntimeException("Failed to initialize key version 1", e);
            }
        }
    }

    public void rotate() {
        // Manually rotate without returning KeyVersion
        rotateKey();
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

    public void deleteVersion(int version) {
        sessionKeys.remove(version);
        try {
            persist();
        } catch (IOException e) {
            logger.error("Failed to persist after key deletion", e);
        }
    }
}