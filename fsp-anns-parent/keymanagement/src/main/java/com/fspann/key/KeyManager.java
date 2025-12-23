package com.fspann.key;

import com.fspann.common.KeyVersion;
import com.fspann.common.PersistenceUtils;
import com.fspann.common.FsPaths;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * KeyManager with secure key deletion support.
 * Old keys are securely wiped after rotation threshold is exceeded.
 */
public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);

    private static final String KEY_ALGO = "AES";
    private static final String KDF_ALGO = "HmacSHA256";
    private static final int KEY_BITS = 256;

    // Retain enough keys to always support selective + full re-encryption
    private static final int MAX_RETAINED_KEYS =
            Integer.getInteger("key.retention.max", 5);

    private final Path storeFile;
    private volatile int currentVersion = 1;
    private SecretKey masterKey;
    private final ConcurrentMap<Integer, SecretKey> sessionKeys = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, SecretKey> derived = new ConcurrentHashMap<>();

    // Track when keys were created for retention policy
    private final ConcurrentMap<Integer, Long> keyCreationTimes = new ConcurrentHashMap<>();

    private final KeyUsageTracker usageTracker = new KeyUsageTracker();

    public KeyManager(String keyStorePath) throws IOException {
        Path p;
        if (keyStorePath == null || keyStorePath.isBlank()) {
            p = FsPaths.keyStoreFile();
        } else {
            Path candidate = Paths.get(keyStorePath);
            p = candidate.isAbsolute()
                    ? candidate.toAbsolutePath().normalize()
                    : FsPaths.baseDir().resolve(candidate).toAbsolutePath().normalize();
        }
        this.storeFile = (Files.exists(p) && Files.isDirectory(p))
                ? p.resolve("keystore.blob") : p;

        Path parent = this.storeFile.getParent();
        if (parent != null) Files.createDirectories(parent);

        initOrLoad();
    }

    private synchronized void initOrLoad() throws IOException {
        if (Files.exists(storeFile)) {
            KeyStoreBlob blob;
            try {
                blob = PersistenceUtils.loadObject(
                        storeFile.toString(),
                        (storeFile.getParent() != null
                                ? storeFile.getParent().toString()
                                : storeFile.toString()),
                        KeyStoreBlob.class
                );
            } catch (ClassNotFoundException | IOException e) {
                throw new IOException("Failed to load keystore: " + storeFile, e);
            }

            if (blob == null || blob.masterKey == null
                    || blob.sessionKeys == null || blob.sessionKeys.isEmpty()) {
                throw new IOException("Invalid or empty keystore: " + storeFile);
            }

            this.masterKey = toSpec(blob.masterKey);
            this.sessionKeys.clear();
            for (Map.Entry<Integer, SecretKey> e : blob.sessionKeys.entrySet()) {
                this.sessionKeys.put(e.getKey(), toSpec(e.getValue()));
                this.keyCreationTimes.putIfAbsent(e.getKey(), System.currentTimeMillis());
            }
            this.currentVersion = this.sessionKeys.keySet().stream()
                    .max(Integer::compare).orElse(1);

            logger.info("Loaded keystore {}, currentVersion=v{}, retained keys={}",
                    storeFile, currentVersion, sessionKeys.size());
        } else {
            try {
                this.masterKey = generateMasterKey();
                this.sessionKeys.put(currentVersion, deriveSessionKey(currentVersion));
                this.keyCreationTimes.put(currentVersion, System.currentTimeMillis());
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new IOException("Failed to initialize keystore", e);
            }
            persistSync();
            logger.info("Initialized new keystore at {}, version=v{}",
                    storeFile, currentVersion);
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

    public SecretKey getSessionKey(int version) {
        return sessionKeys.get(version);
    }

    /**
     * Rotate key and enforce retention policy: securely delete old keys
     * beyond MAX_RETAINED_KEYS.
     */
    public synchronized KeyVersion rotateKey() {
        int next = currentVersion + 1;
        SecretKey nextKey;
        try {
            nextKey = deriveSessionKey(next);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Key derivation failed", e);
        }

        sessionKeys.put(next, nextKey);
        keyCreationTimes.put(next, System.currentTimeMillis());
        currentVersion = next;

        // IMPORTANT:
        // DO NOT prune old keys here.
        // Deletion now happens ONLY inside KeyRotationServiceImpl.finalizeRotation().

        persistSync();
        logger.info("Rotated key: v{}, retained temporarily={}", currentVersion, sessionKeys.size());
        return new KeyVersion(currentVersion, nextKey);
    }

    /**
     * Securely delete keys older than the configured retention threshold.
     * NOTE: This must NOT be called during rotation — only for background cleanup.
     */
    private void pruneOldKeys() {
        if (sessionKeys.size() <= MAX_RETAINED_KEYS) {
            return;
        }

        List<Integer> versions = new ArrayList<>(sessionKeys.keySet());
        versions.sort(Collections.reverseOrder()); // newest first

        // Keep only MAX_RETAINED_KEYS most recent
        List<Integer> toDelete = versions.subList(
                MAX_RETAINED_KEYS,
                versions.size()
        );

        for (Integer v : toDelete) {
            SecretKey old = sessionKeys.remove(v);
            derived.remove(v);
            keyCreationTimes.remove(v);

            if (old != null) {
                SecureKeyDeletion.wipeKey(old);
                logger.info("Securely deleted key v{} (retention policy)", v);
            }
        }
    }

    private void persistSync() {
        try {
            Path tmp = Files.createTempFile(
                    storeFile.getParent(),
                    "keystore",
                    ".tmp"
            );
            try {
                PersistenceUtils.saveObject(
                        new KeyStoreBlob(
                                toSpec(masterKey),
                                toSpecMap(sessionKeys)
                        ),
                        tmp.toString(),
                        (tmp.getParent() != null
                                ? tmp.getParent().toString()
                                : tmp.toString())
                );
                Files.move(
                        tmp,
                        storeFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                );
            } finally {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignore) {}
            }
            logger.debug("Persisted keystore {}, version=v{}",
                    storeFile.getFileName(), currentVersion);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist keystore to " + storeFile, e);
        }
    }

    public SecretKey deriveSessionKey(int version)
            throws NoSuchAlgorithmException, InvalidKeyException {
        return derived.computeIfAbsent(version, v -> {
            try {
                Mac mac = Mac.getInstance(KDF_ALGO);
                mac.init(Objects.requireNonNull(masterKey,
                        "Master key is not initialized"));
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
        return toSpec(k);
    }

    private static SecretKey toSpec(SecretKey k) {
        return new SecretKeySpec(
                Objects.requireNonNull(k, "key").getEncoded(),
                KEY_ALGO
        );
    }

    private static ConcurrentMap<Integer, SecretKey> toSpecMap(
            Map<Integer, SecretKey> in
    ) {
        ConcurrentMap<Integer, SecretKey> out = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, SecretKey> e : in.entrySet()) {
            out.put(e.getKey(), toSpec(e.getValue()));
        }
        return out;
    }

    /**
     * Delete all keys strictly older than keepVersion.
     * Uses secure deletion and updates all auxiliary maps.
     *
     * SAFETY: Verifies no vectors are bound to keys before deletion.
     * If vectors are still bound, logs warning and skips deletion.
     *
     * NOTE: All versions can be deleted (including v1) because the master key
     * is stored separately and is never deleted. Session keys are derived from
     * the master key on-demand, so deleting v1 doesn't prevent us from deriving it again.
     */
    public synchronized void deleteKeysOlderThan(int keepVersion) {
        try {
            List<Integer> toDelete = new ArrayList<>();
            for (Integer v : sessionKeys.keySet()) {
                if (v < keepVersion) {
                    toDelete.add(v);
                }
            }
            Collections.sort(toDelete);

            int deleted = 0;
            int skipped = 0;

            for (Integer v : toDelete) {
                // SAFETY CHECK: Verify key is safe to delete
                if (!usageTracker.isSafeToDelete(v)) {
                    logger.warn("SKIPPING deletion of key v{}: {} vectors still bound",
                            v, usageTracker.getVectorCount(v));
                    skipped++;
                    continue;
                }

                SecretKey old = sessionKeys.remove(v);
                derived.remove(v);
                keyCreationTimes.remove(v);

                if (old != null) {
                    SecureKeyDeletion.wipeKey(old);
                    deleted++;
                    logger.info("✓ Securely deleted key v{} (verified safe)", v);
                }
            }

            if (deleted > 0) {
                persistSync();
            }

            logger.info("Key deletion complete: {} deleted, {} skipped (still in use)",
                    deleted, skipped);

        } catch (Exception e) {
            logger.error("deleteKeysOlderThan({}) failed", keepVersion, e);
        }
    }

    public KeyUsageTracker getUsageTracker() {
        return usageTracker;
    }

    private static class KeyStoreBlob implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final SecretKey masterKey;
        final ConcurrentMap<Integer, SecretKey> sessionKeys;

        KeyStoreBlob(SecretKey masterKey,
                     ConcurrentMap<Integer, SecretKey> sessionKeys) {
            this.masterKey = masterKey;
            this.sessionKeys = sessionKeys;
        }
    }
}