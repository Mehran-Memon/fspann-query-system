package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.RocksDBMetadataManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

public class AesGcmCryptoService implements CryptoService {
    private static final Logger logger = LoggerFactory.getLogger(AesGcmCryptoService.class);

    private final MeterRegistry registry;
    private final Timer encryptTimer;
    private final Timer decryptTimer;

    private final RocksDBMetadataManager metadataManager;
    private volatile KeyVersion cachedVersion;

    private KeyLifeCycleService keyService;

    public AesGcmCryptoService(MeterRegistry registry,
                               KeyLifeCycleService keyService,
                               RocksDBMetadataManager metadataManager) {
        this.registry = (registry != null) ? registry : new SimpleMeterRegistry();
        this.keyService = Objects.requireNonNull(keyService, "KeyLifeCycleService must not be null");
        this.metadataManager = (metadataManager != null) ? metadataManager : createDefaultMetadataManager();

        this.encryptTimer = Timer.builder("fspann.crypto.encrypt")
                .description("AES-GCM encryption latency")
                .register(this.registry);

        this.decryptTimer = Timer.builder("fspann.crypto.decrypt")
                .description("AES-GCM decryption latency")
                .register(this.registry);
    }

    /** Optional helper for other modules/tests that want the registry. */
    public MeterRegistry getMeterRegistry() { return registry; }

    // ---------------------------------------------------------------------
    // CryptoService API
    // ---------------------------------------------------------------------

    /**
     * Encrypt a query-like point with an explicitly provided key.
     * NOTE: This is typically used to protect ephemeral query vectors.
     * It does NOT bind AAD to ID/version/dim (kept stateless for interop).
     */
    @Override
    public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(vector, "vector");
        Objects.requireNonNull(key, "key");
        if (!id.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid ID format");
        }

        return encryptTimer.record(() -> {
            try {
                KeyVersion cur = keyService.getCurrentVersion();
                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key); // stateless (no AAD)

                // No metadata persistence here; intended for ephemeral usage.
                return new EncryptedPoint(id, 0, iv, ciphertext, cur.getVersion(), vector.length, null);
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point: " + id, e);
            }
        });
    }

    /**
     * Encrypt a stored point with the CURRENT key version, binding AAD = (id, version, dim).
     */
    @Override
    public EncryptedPoint encrypt(String id, double[] vector) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        Objects.requireNonNull(vector, "Vector cannot be null");
        if (!id.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid ID format: only alphanumeric, underscore, and hyphen allowed");
        }
        if (vector.length == 0) throw new IllegalArgumentException("Vector cannot be empty");
        for (double v : vector) {
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException("Vector contains invalid values (NaN or Infinite)");
            }
        }

        return encryptTimer.record(() -> {
            try {
                KeyVersion current = keyService.getCurrentVersion();
                if (cachedVersion == null || cachedVersion.getVersion() != current.getVersion()) {
                    synchronized (this) {
                        if (cachedVersion == null || cachedVersion.getVersion() != current.getVersion()) {
                            cachedVersion = current;
                        }
                    }
                }

                SecretKey key = cachedVersion.getKey();
                int dim = vector.length;
                byte[] iv = EncryptionUtils.generateIV();
                byte[] aad = aadForStored(id, cachedVersion.getVersion(), dim);
                byte[] ciphertext = EncryptionUtils.encryptVectorWithAad(vector, iv, key, aad);

                EncryptedPoint point = new EncryptedPoint(
                        id, 0, iv, ciphertext, cachedVersion.getVersion(), dim, null
                );

                // Persist minimal metadata (version/dim) for filtering and rotation sanity
                metadataManager.updateVectorMetadata(id, Map.of(
                        "version", String.valueOf(cachedVersion.getVersion()),
                        "dim", String.valueOf(dim)
                ));
                logger.debug("Encrypted point {} with version {} (dim={})", id, cachedVersion.getVersion(), dim);
                return point;
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point: " + id, e);
            }
        });
    }

    /**
     * Decrypt an existing stored point with a supplied key (usually by version),
     * enforcing AAD = (id, version, dim).
     */
    @Override
    public double[] decryptFromPoint(EncryptedPoint pt, SecretKey key) {
        Objects.requireNonNull(pt, "pt");
        Objects.requireNonNull(key, "key");

        logger.debug("Decrypting point: id={}, version={}, dim={}", pt.getId(), pt.getVersion(), pt.getVectorLength());
        return decryptTimer.record(() -> {
            try {
                byte[] aad = aadForStored(pt.getId(), pt.getVersion(), pt.getVectorLength());
                return EncryptionUtils.decryptVectorWithAad(pt.getCiphertext(), pt.getIv(), key, aad);
            } catch (GeneralSecurityException e) {
                logger.error("Decryption failed for point {} (version {}): {}", pt.getId(), pt.getVersion(), e.getMessage());
                throw new CryptoException("Decryption failed (AAD mismatch or invalid key)", e);
            }
        });
    }

    /**
     * Stateless vector encrypt (explicit key+IV). No AAD by design (query traffic).
     */
    @Override
    public byte[] encrypt(double[] vector, SecretKey key, byte[] iv) {
        Objects.requireNonNull(vector, "vector");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(iv, "iv");

        return encryptTimer.record(() -> {
            try {
                logger.debug("Encrypting vector with IV: {}", Base64.getEncoder().encodeToString(iv));
                return EncryptionUtils.encryptVector(vector, iv, key);
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for vector", e);
                throw new CryptoException("Encryption failed", e);
            }
        });
    }

    /**
     * Stateless vector decrypt (explicit key+IV) used for query traffic. No AAD.
     */
    @Override
    public double[] decryptQuery(byte[] ciphertext, byte[] iv, SecretKey key) {
        Objects.requireNonNull(ciphertext, "ciphertext");
        Objects.requireNonNull(iv, "iv");
        Objects.requireNonNull(key, "key");

        return decryptTimer.record(() -> {
            try {
                logger.debug("Decrypting query with IV: {}", Base64.getEncoder().encodeToString(iv));
                return EncryptionUtils.decryptVector(ciphertext, iv, key);
            } catch (GeneralSecurityException e) {
                logger.error("Query decryption failed", e);
                throw new CryptoException("Query decryption failed", e);
            }
        });
    }

    /**
     * Re-encrypt a stored point to the current key version (forward security).
     * Decrypt with AAD(old id, old ver, dim), re-encrypt with AAD(new id, new ver, dim).
     */
    @Override
    public EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey, byte[] newIv) {
        Objects.requireNonNull(pt, "pt");
        Objects.requireNonNull(newKey, "newKey");
        Objects.requireNonNull(newIv, "newIv");

        return encryptTimer.record(() -> {
            int oldVersion = pt.getVersion();
            SecretKey oldKey;
            try {
                oldKey = keyService.getVersion(oldVersion).getKey();
            } catch (IllegalArgumentException e) {
                logger.warn("Skipping re-encryption for point {}: missing key version {}", pt.getId(), oldVersion);
                throw new CryptoException("Old key version not found: " + oldVersion, e);
            }

            try {
                byte[] aadOld = aadForStored(pt.getId(), pt.getVersion(), pt.getVectorLength());
                double[] plaintext = EncryptionUtils.decryptVectorWithAad(pt.getCiphertext(), pt.getIv(), oldKey, aadOld);

                int newVersion = keyService.getCurrentVersion().getVersion();
                if (newVersion == pt.getVersion()) {
                    logger.debug("Skipping re-encryption for point {}: already at latest version v{}", pt.getId(), newVersion);
                    return pt;
                }

                byte[] aadNew = aadForStored(pt.getId(), newVersion, pt.getVectorLength());
                byte[] ciphertext = EncryptionUtils.encryptVectorWithAad(plaintext, newIv, newKey, aadNew);

                EncryptedPoint reEncrypted = new EncryptedPoint(
                        pt.getId(), pt.getShardId(), newIv, ciphertext, newVersion, pt.getVectorLength(), pt.getBuckets()
                );

                metadataManager.updateVectorMetadata(pt.getId(), Map.of(
                        "version", String.valueOf(newVersion),
                        "dim", String.valueOf(pt.getVectorLength())
                ));
                logger.debug("Re-encrypted point {} from v{} to v{}", pt.getId(), oldVersion, newVersion);
                return reEncrypted;
            } catch (GeneralSecurityException e) {
                logger.error("Re-encryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Re-encryption failed", e);
            }
        });
    }

    @Override
    public byte[] generateIV() { return EncryptionUtils.generateIV(); }

    public void setKeyService(KeyLifeCycleService keyService) { this.keyService = keyService; }

    @Override
    public KeyLifeCycleService getKeyService() { return this.keyService; }

    // ---------------------------------------------------------------------

    private static byte[] aadForStored(String id, int version, int dim) {
        // Compact, deterministic AAD: "id:<id>|v:<version>|d:<dim>"
        // Do NOT include secrets; this only binds invariants.
        String s = "id:" + id + "|v:" + version + "|d:" + dim;
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public static class CryptoException extends RuntimeException {
        public CryptoException(String message, Throwable cause) { super(message, cause); }
    }

    private static RocksDBMetadataManager createDefaultMetadataManager() {
        try {
            return RocksDBMetadataManager.create("metadata/rocksdb", "metadata/points");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RocksDBMetadataManager", e);
        }
    }
}
