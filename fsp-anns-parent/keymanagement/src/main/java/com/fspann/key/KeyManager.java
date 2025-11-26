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
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * KeyManager
 * -----------------------------------------------------------------------------
 * - Keeps a random master key (AES-256) and derives per-version session keys via HMAC-SHA256(K_master, version).
 * - Persists the store atomically to a single file (or <dir>/keystore.blob if a directory is passed).
 * - Strict load semantics: any malformed/corrupted store throws IOException (fail-closed).
 * - Session keys and master key are stored in provider-agnostic SecretKeySpec form.
 */
public class KeyManager {
    private static final Logger logger = LoggerFactory.getLogger(KeyManager.class);

    private static final String KEY_ALGO = "AES";
    private static final String KDF_ALGO  = "HmacSHA256";
    private static final int KEY_BITS = 256;

    /** Exact file to persist the keystore. If a directory path is provided, we store inside it as "keystore.blob". */
    private final Path storeFile;

    /** Monotonically increasing key version; v1 exists at init. */
    private volatile int currentVersion = 1;

    /** Random master key, used only to derive session keys. */
    private SecretKey masterKey;

    /** Cache of session keys by version (derived from master); persisted for fast startup. */
    private final ConcurrentMap<Integer, SecretKey> sessionKeys = new ConcurrentHashMap<>();

    /** Optional memoized derivations (same as sessionKeys, but kept distinct in case you change persistence). */
    private final ConcurrentMap<Integer, SecretKey> derived = new ConcurrentHashMap<>();

    public KeyManager(String keyStorePath) throws IOException {
        // Resolve final store path
        Path p;
        if (keyStorePath == null || keyStorePath.isBlank()) {
            p = FsPaths.keyStoreFile();
        } else {
            Path candidate = Paths.get(keyStorePath);
            p = candidate.isAbsolute()
                    ? candidate.toAbsolutePath().normalize()
                    : FsPaths.baseDir().resolve(candidate).toAbsolutePath().normalize();
        }
        this.storeFile = (Files.exists(p) && Files.isDirectory(p)) ? p.resolve("keystore.blob") : p;

        Path parent = this.storeFile.getParent();
        if (parent != null) Files.createDirectories(parent);

        initOrLoad();
    }

    private synchronized void initOrLoad() throws IOException {
        if (Files.exists(storeFile)) {
            // Strict, fail-closed load
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

            if (blob == null || blob.masterKey == null || blob.sessionKeys == null || blob.sessionKeys.isEmpty()) {
                throw new IOException("Invalid or empty keystore: " + storeFile);
            }

            // Ensure provider-agnostic SecretKeySpec
            this.masterKey = toSpec(blob.masterKey);
            this.sessionKeys.clear();
            for (Map.Entry<Integer, SecretKey> e : blob.sessionKeys.entrySet()) {
                this.sessionKeys.put(e.getKey(), toSpec(e.getValue()));
            }
            this.currentVersion = this.sessionKeys.keySet().stream().max(Integer::compare).orElse(1);
            logger.info("Loaded keystore {}, currentVersion=v{}", storeFile, currentVersion);
        } else {
            // Fresh keystore: new master, derive v1
            try {
                this.masterKey = generateMasterKey();
                this.sessionKeys.put(currentVersion, deriveSessionKey(currentVersion));
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new IOException("Failed to initialize keystore", e);
            }
            persistSync();
            logger.info("Initialized new keystore at {}, version=v{}", storeFile, currentVersion);
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
    public SecretKey getSessionKey(int version) { return sessionKeys.get(version); }

    /** Synchronized so concurrent calls produce strictly increasing versions (no lost updates). */
    public synchronized KeyVersion rotateKey() throws NoSuchAlgorithmException, InvalidKeyException {
        int next = currentVersion + 1;
        SecretKey nextKey = deriveSessionKey(next);
        sessionKeys.put(next, nextKey);

        // ✅ DELETE old session key (true forward security)
        if (currentVersion > 1) {
            SecretKey oldKey = sessionKeys.remove(currentVersion - 1);
            if (oldKey != null) {
                // Overwrite key material before GC
                Arrays.fill(oldKey.getEncoded(), (byte) 0);
            }
        }
        currentVersion = next;
        persistSync(); // Only persist K_master + K_current
        return new KeyVersion(currentVersion, nextKey);
    }

    private void persistSync() {
        try {
            // Write atomically: tmp -> move replace
            Path tmp = Files.createTempFile(storeFile.getParent(), "keystore", ".tmp");
            try {
                PersistenceUtils.saveObject(
                        new KeyStoreBlob(toSpec(masterKey), toSpecMap(sessionKeys)),
                        tmp.toString(),
                        (tmp.getParent() != null ? tmp.getParent().toString() : tmp.toString())
                );
                Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                // If move failed, ensure tmp is removed
                try { Files.deleteIfExists(tmp); } catch (IOException ignore) {}
            }
            logger.debug("Persisted keystore {}, version=v{}", storeFile.getFileName(), currentVersion);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist keystore to " + storeFile, e);
        }
    }

    /** Derive AES key for a given version using HMAC-SHA256(masterKey, int32(version)). */
    public SecretKey deriveSessionKey(int version) throws NoSuchAlgorithmException, InvalidKeyException {
        return derived.computeIfAbsent(version, v -> {
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
        return toSpec(k);
    }

    private static SecretKey toSpec(SecretKey k) {
        return new SecretKeySpec(Objects.requireNonNull(k, "key").getEncoded(), KEY_ALGO);
    }
    
    private static ConcurrentMap<Integer, SecretKey> toSpecMap(Map<Integer, SecretKey> in) {
        ConcurrentMap<Integer, SecretKey> out = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, SecretKey> e : in.entrySet()) out.put(e.getKey(), toSpec(e.getValue()));
        return out;
    }

    /** Serializable blob for disk (fail-closed on load). */
    private static class KeyStoreBlob implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final SecretKey masterKey;
        final ConcurrentMap<Integer, SecretKey> sessionKeys;
        KeyStoreBlob(SecretKey masterKey, ConcurrentMap<Integer, SecretKey> sessionKeys) {
            this.masterKey = masterKey;
            this.sessionKeys = sessionKeys;
        }
    }
}
