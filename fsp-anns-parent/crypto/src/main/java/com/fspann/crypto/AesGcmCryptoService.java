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
import java.util.Map;
import java.util.Objects;
import java.util.Base64;

public class AesGcmCryptoService implements CryptoService {
    private static final Logger logger = LoggerFactory.getLogger(AesGcmCryptoService.class);

    private final MeterRegistry registry;
    private final Timer encryptTimer;
    private final Timer decryptTimer;
    private KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadataManager;
    private volatile KeyVersion cachedVersion;

    public AesGcmCryptoService(MeterRegistry registry,
                               KeyLifeCycleService keyService,
                               RocksDBMetadataManager metadataManager) {
        this.registry = (registry != null) ? registry : new SimpleMeterRegistry();
        this.keyService = Objects.requireNonNull(keyService, "KeyLifeCycleService must not be null");
        this.metadataManager = (metadataManager != null) ? metadataManager : createDefaultMetadataManager();

        this.encryptTimer = Timer.builder("fspann.crypto.encrypt.time")
                .description("AES-GCM encryption latency")
                .register(this.registry);

        this.decryptTimer = Timer.builder("fspann.crypto.decrypt.time")
                .description("AES-GCM decryption latency")
                .register(this.registry);
    }

    // encryptToPoint: stop writing metadata, keep version stamp, warn if non-current key used
    @Override
    public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
        if (!id.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid ID format");
        }
        return encryptTimer.record(() -> {
            try {
                KeyVersion cur = keyService.getCurrentVersion();
                // best-effort: log if caller didn't pass the current key (providers may not expose getEncoded)
                try {
                    byte[] curBytes = cur.getKey().getEncoded();
                    byte[] argBytes = key.getEncoded();
                    if (curBytes != null && argBytes != null && !java.util.Arrays.equals(curBytes, argBytes)) {
                        logger.warn("encryptToPoint called with a non-current key; stamping current version v{}", cur.getVersion());
                    }
                } catch (Throwable ignore) {}

                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key);

                // IMPORTANT: do NOT write metadata here (query tokens are ephemeral)
                EncryptedPoint point = new EncryptedPoint(id, 0, iv, ciphertext, cur.getVersion(), vector.length, null);
                logger.debug("Encrypted query point {} with key version {}", id, cur.getVersion());
                return point;
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point: " + id, e);
            }
        });
    }

    @Override
    public EncryptedPoint encrypt(String id, double[] vector) {
        return encryptTimer.record(() -> {
            try {
                Objects.requireNonNull(id, "Point ID cannot be null");
                if (!id.matches("[a-zA-Z0-9_-]+")) {
                    throw new IllegalArgumentException("Invalid ID format: only alphanumeric, underscore, and hyphen allowed");
                }
                Objects.requireNonNull(vector, "Vector cannot be null");
                if (vector.length == 0) {
                    throw new IllegalArgumentException("Vector cannot be empty");
                }
                for (double v : vector) {
                    if (Double.isNaN(v) || Double.isInfinite(v)) {
                        throw new IllegalArgumentException("Vector contains invalid values (NaN or Infinite)");
                    }
                }

                KeyVersion current = keyService.getCurrentVersion();
                if (cachedVersion == null || cachedVersion.getVersion() != current.getVersion()) {
                    synchronized (this) {
                        if (cachedVersion == null || cachedVersion.getVersion() != current.getVersion()) {
                            cachedVersion = current;
                        }
                    }
                }
                SecretKey key = cachedVersion.getKey();
                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key);
                EncryptedPoint point = new EncryptedPoint(id, 0, iv, ciphertext, cachedVersion.getVersion(), vector.length, null);
                metadataManager.updateVectorMetadata(id, Map.of("version", String.valueOf(cachedVersion.getVersion())));
                logger.debug("Encrypted point {} with version {}", id, cachedVersion.getVersion());
                return point;
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point: " + id, e);
            }
        });
    }

    // decryptFromPoint: lower to DEBUG to avoid noisy/sensitive INFO logs
    @Override
    public double[] decryptFromPoint(EncryptedPoint pt, SecretKey key) {
        logger.debug("Decrypting point: id={}, version={}", pt.getId(), pt.getVersion());
        return decryptTimer.record(() -> {
            try {
                return EncryptionUtils.decryptVector(pt.getCiphertext(), pt.getIv(), key);
            } catch (GeneralSecurityException e) {
                logger.error("Decryption failed (possibly due to stale key) for point {}: {}", pt.getId(), e.getMessage());
                throw new CryptoException("Forward security breach: invalid decryption key", e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public byte[] encrypt(double[] vector, SecretKey key, byte[] iv) {
        return encryptTimer.record(() -> {
            try {
                // Avoid logging raw key bytes
                logger.debug("Encrypting vector with IV: {}", Base64.getEncoder().encodeToString(iv));
                return EncryptionUtils.encryptVector(vector, iv, key);
            } catch (GeneralSecurityException e) {
                logger.error("Encryption failed for vector", e);
                throw new CryptoException("Encryption failed", e);
            }
        });
    }

    @Override
    public double[] decryptQuery(byte[] ciphertext, byte[] iv, SecretKey key) {
        return decryptTimer.record(() -> {
            try {
                logger.debug("Decrypting query with IV: {}", Base64.getEncoder().encodeToString(iv));
                return EncryptionUtils.decryptVector(ciphertext, iv, key);
            } catch (GeneralSecurityException e) {
                logger.error("Query decryption failed", e);
                throw new CryptoException("Query decryption failed", e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey, byte[] newIv) {
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
                double[] plaintext = EncryptionUtils.decryptVector(pt.getCiphertext(), pt.getIv(), oldKey);
                byte[] ciphertext = EncryptionUtils.encryptVector(plaintext, newIv, newKey);
                int newVersion = keyService.getCurrentVersion().getVersion();

                if (newVersion == pt.getVersion()) {
                    logger.debug("Skipping re-encryption for point {}: already at latest version v{}", pt.getId(), newVersion);
                    return pt;
                }

                EncryptedPoint reEncrypted = new EncryptedPoint(
                        pt.getId(), pt.getShardId(), newIv, ciphertext, newVersion, pt.getVectorLength(), null
                );

                metadataManager.updateVectorMetadata(pt.getId(), Map.of("version", String.valueOf(newVersion)));
                logger.debug("Re-encrypted point {} from v{} to v{}", pt.getId(), oldVersion, newVersion);
                return reEncrypted;
            } catch (GeneralSecurityException e) {
                logger.error("Re-encryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Re-encryption failed", e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public byte[] generateIV() {
        return EncryptionUtils.generateIV();
    }

    public static class CryptoException extends RuntimeException {
        public CryptoException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public void setKeyService(KeyLifeCycleService keyService) {
        this.keyService = keyService;
    }

    @Override
    public KeyLifeCycleService getKeyService() {
        return this.keyService;
    }

    private static RocksDBMetadataManager createDefaultMetadataManager() {
        try {
            // Use the factory since the constructor is private
            return RocksDBMetadataManager.create("metadata/rocksdb", "metadata/points");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RocksDBMetadataManager", e);
        }
    }
}
