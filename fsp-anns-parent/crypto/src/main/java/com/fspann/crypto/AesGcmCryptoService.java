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

    public AesGcmCryptoService(MeterRegistry registry, KeyLifeCycleService keyService, RocksDBMetadataManager metadataManager) {
        this.registry = registry != null ? registry : new SimpleMeterRegistry();
        this.keyService = Objects.requireNonNull(keyService, "KeyLifeCycleService must not be null");
        this.metadataManager = metadataManager != null ? metadataManager : createDefaultMetadataManager();

        this.encryptTimer = Timer.builder("fspann.crypto.encrypt.time")
                .description("AES-GCM encryption latency")
                .register(this.registry);

        this.decryptTimer = Timer.builder("fspann.crypto.decrypt.time")
                .description("AES-GCM decryption latency")
                .register(this.registry);
    }

    @Override
    public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
        return encryptTimer.record(() -> {
            try {
                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key);
                int version = keyService.getCurrentVersion().getVersion();
                EncryptedPoint point = new EncryptedPoint(id, 0, iv, ciphertext, version, vector.length);
                metadataManager.updateVectorMetadata(id, Map.of("version", String.valueOf(version)));
                logger.debug("Encrypted point {} with key version {}", id, version);
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
                keyService.rotateIfNeeded();  // Ensure key is rotated if needed
                KeyVersion version = keyService.getCurrentVersion();
                SecretKey key = version.getKey();

                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key);

                EncryptedPoint point = new EncryptedPoint(id, 0, iv, ciphertext, version.getVersion(), vector.length);
                metadataManager.updateVectorMetadata(id, Map.of("version", String.valueOf(version.getVersion())));

                logger.debug("Encrypted (auto-version) point {} with version {}", id, version.getVersion());
                return point;
            } catch (GeneralSecurityException e) {
                logger.error("Auto-encryption failed for point {}", id, e);
                throw new CryptoException("Auto-encryption failed for point: " + id, e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    @Override
    public double[] decryptFromPoint(EncryptedPoint pt, SecretKey key) {
        logger.info("Decrypting point: id={}, version={}, IV={}", pt.getId(), pt.getVersion(), Base64.getEncoder().encodeToString(pt.getIv()));
        return decryptTimer.record(() -> {
            logger.info("Attempting decryption for point {} with key version {} and key: {}",
                    pt.getId(), pt.getVersion(), Base64.getEncoder().encodeToString(key.getEncoded()));
            try {
                logger.debug("Decrypting point {} with key version {}", pt.getId(), pt.getVersion());
                return EncryptionUtils.decryptVector(pt.getCiphertext(), pt.getIv(), key);
            } catch (GeneralSecurityException e) {
                logger.error("Decryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Decryption failed for point: " + pt.getId(), e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public byte[] encrypt(double[] vector, SecretKey key, byte[] iv) {
        return encryptTimer.record(() -> {
            try {
                logger.debug("Encrypting vector with IV: {}, key: {}",
                        Base64.getEncoder().encodeToString(iv),
                        Base64.getEncoder().encodeToString(key.getEncoded()));
                byte[] ciphertext = EncryptionUtils.encryptVector(vector, iv, key);
                logger.debug("Encrypted ciphertext: {}", Base64.getEncoder().encodeToString(ciphertext));
                return ciphertext;
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
                logger.debug("Decrypting query with IV: {}, key: {}, ciphertext: {}",
                        Base64.getEncoder().encodeToString(iv),
                        Base64.getEncoder().encodeToString(key.getEncoded()),
                        Base64.getEncoder().encodeToString(ciphertext));
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
                EncryptedPoint reEncrypted = new EncryptedPoint(
                        pt.getId(), pt.getShardId(), newIv, ciphertext, newVersion, pt.getVectorLength()
                );

                metadataManager.updateVectorMetadata(pt.getId(), Map.of("version", String.valueOf(newVersion)));
                logger.debug("Re-encrypted (custom IV) point {} from v{} to v{}", pt.getId(), oldVersion, newVersion);
                return reEncrypted;
            } catch (IOException | GeneralSecurityException e) {
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
            return new RocksDBMetadataManager("metadata/rocksdb");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RocksDBMetadataManager", e);
        }
    }

}