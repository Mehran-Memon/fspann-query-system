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
import java.nio.file.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);

    private static final String KEY_ALGO = "AES";
    private static final String KDF_ALGO  = "HmacSHA256";
    private static final int KEY_BITS = 256;

    /** Exact file to persist the keystore. If a directory path is provided, we store inside it as "keystore.blob". */
    private final Path storeFile;

    private volatile int currentVersion = 1;
    private SecretKey masterKey;

    private final ConcurrentMap<Integer, SecretKey> sessionKeys = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, SecretKey> derivedKeys = new ConcurrentHashMap<>();

    public KeyManager(String keyStorePath) throws IOException {
        if (keyStorePath == null) throw new IllegalArgumentException("keyStorePath must not be null");

        Path p = Paths.get(keyStorePath).toAbsolutePath().normalize();
        if (Files.exists(p) && Files.isDirectory(p)) {
            this.storeFile = p.resolve("keystore.blob");
        } else {
            this.storeFile = p;
        }

        Path parent = this.storeFile.getParent();
        if (parent != null) Files.createDirectories(parent);

        initOrLoad();
    }

    private synchronized void initOrLoad() throws IOException {
        if (Files.exists(storeFile)) {
            // Strict load with validation -> any weirdness becomes IOException (fulfills tamper test).
            KeyStoreBlob blob;
            try {
                blob = PersistenceUtils.loadObject(
                        storeFile.toString(),
                        (storeFile.getParent() != null ? storeFile.getParent().toString() : storeFile.toString()),
                        KeyStoreBlob.class
                );
            } catch (ClassNotFoundException | IOException e) {
                throw new IOException("Failed to load keystore: " + storeFile, e);
            }

            if (blob == null || blob.getMasterKey() == null || blob.getSessionKeys() == null || blob.getSessionKeys().isEmpty()) {
                throw new IOException("Invalid or empty keystore: " + storeFile);
            }

            this.masterKey = blob.getMasterKey();
            this.sessionKeys.clear();
            this.sessionKeys.putAll(blob.getSessionKeys());
            this.currentVersion = this.sessionKeys.keySet().stream().max(Integer::compare).orElse(1);
            logger.debug("Loaded keystore {}, currentVersion={}", storeFile, currentVersion);
        } else {
            // Fresh keystore
            try {
                this.masterKey = generateMasterKey();
                this.sessionKeys.put(currentVersion, deriveSessionKey(currentVersion));
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new IOException("Failed to initialize keystore", e);
            }
            persistSync(); // sync write fixes EOF on immediate reload in tests
            logger.debug("Initialized new keystore at {}, version={}", storeFile, currentVersion);
        }
    }

    public KeyVersion getCurrentVersion() {
        return new KeyVersion(currentVersion, sessionKeys.get(currentVersion));
    }

    public KeyVersion getPreviousVersion() {
        return sessionKeys.keySet().stream()
                .filter(v -> v < currentVersion)
                .max(Integer::compare)
                .map(v -> new KeyVersion(v, sessionKeys.get(v)))
                .orElse(null);
    }

    /** Exposed for other components (e.g., rotation service). */
    public SecretKey getSessionKey(int version) {
        return sessionKeys.get(version);
    }

    /** Synchronized so 100 concurrent calls result in +100 versions, deterministically. */
    public synchronized KeyVersion rotateKey() {
        int next = currentVersion + 1;
        SecretKey nextKey;
        try {
            nextKey = deriveSessionKey(next);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Key derivation failed", e);
        }
        sessionKeys.put(next, nextKey);
        currentVersion = next;

        persistSync(); // sync persist avoids EOF race on immediate reload
        return new KeyVersion(currentVersion, nextKey);
    }

    private void persistSync() {
        try {
            PersistenceUtils.saveObject(
                    new KeyStoreBlob(masterKey, sessionKeys),
                    storeFile.toString(),
                    (storeFile.getParent() != null ? storeFile.getParent().toString() : storeFile.toString())
            );
            logger.debug("Persisted keystore {}, version={}", storeFile.getFileName(), currentVersion);
        } catch (IOException e) {
            // Propagate as unchecked: persist must succeed to keep store consistent in tests.
            throw new RuntimeException("Failed to persist keystore to " + storeFile, e);
        }
    }

    public SecretKey deriveSessionKey(int version) throws NoSuchAlgorithmException, InvalidKeyException {
        return derivedKeys.computeIfAbsent(version, v -> {
            try {
                Mac mac = Mac.getInstance(KDF_ALGO);
                mac.init(Objects.requireNonNull(masterKey, "Master key is not initialized"));
                byte[] salt = ByteBuffer.allocate(4).putInt(v).array();
                byte[] out = mac.doFinal(salt);
                byte[] keyBytes = new byte[KEY_BITS / 8];
                System.arraycopy(out, 0, keyBytes, 0, keyBytes.length);
                return new SecretKeySpec(keyBytes, KEY_ALGO);
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private SecretKey generateMasterKey() throws NoSuchAlgorithmException {
        KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGO);
        kg.init(KEY_BITS, SecureRandom.getInstanceStrong());
        SecretKey k = kg.generateKey();
        // Ensure the stored key is serializable across providers
        return new javax.crypto.spec.SecretKeySpec(k.getEncoded(), KEY_ALGO);
    }

    /** Simple serializable blob for disk. */
    private static class KeyStoreBlob implements java.io.Serializable {
        private final SecretKey masterKey;
        private final ConcurrentMap<Integer, SecretKey> sessionKeys;
        KeyStoreBlob(SecretKey masterKey, ConcurrentMap<Integer, SecretKey> sessionKeys) {
            this.masterKey = masterKey;
            this.sessionKeys = sessionKeys;
        }
        SecretKey getMasterKey() { return masterKey; }
        ConcurrentMap<Integer, SecretKey> getSessionKeys() { return sessionKeys; }
    }
}
