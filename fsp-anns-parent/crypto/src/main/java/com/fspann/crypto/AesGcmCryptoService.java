package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.MetadataManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.Map;
import java.util.Objects;

/**
 * AES-GCM implementation for secure vector encryption and re-encryption.
 */
public class AesGcmCryptoService implements CryptoService {
    private static final Logger logger = LoggerFactory.getLogger(AesGcmCryptoService.class);

    private final MeterRegistry registry;
    private final Timer encryptTimer;
    private final Timer decryptTimer;
    private final KeyLifeCycleService keyService;
    private final MetadataManager metadataManager;

    public AesGcmCryptoService() {
        this(new SimpleMeterRegistry(), null, null);
    }

    public AesGcmCryptoService(MeterRegistry registry, KeyLifeCycleService keyService, MetadataManager metadataManager) {
        this.registry = registry != null ? registry : new SimpleMeterRegistry();
        this.keyService = Objects.requireNonNull(keyService, "KeyLifeCycleService must not be null");
        this.metadataManager = Objects.requireNonNullElseGet(metadataManager, MetadataManager::new);

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
                EncryptedPoint point = new EncryptedPoint(id, 0, iv, ciphertext, version);
                metadataManager.updateVectorMetadata(id, Map.of("version", String.valueOf(version)));
                return point;
            } catch (Exception e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point: " + id, e);
            }
        });
    }

    @Override
    public double[] decryptFromPoint(EncryptedPoint pt, SecretKey key) {
        return decryptTimer.record(() -> {
            try {
                return EncryptionUtils.decryptVector(pt.getCiphertext(), pt.getIv(), key);
            } catch (Exception e) {
                logger.error("Decryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Decryption failed for point: " + pt.getId(), e);
            }
        });
    }

    @Override
    public double[] decryptQuery(byte[] ciphertext, byte[] iv, SecretKey key) {
        return decryptTimer.record(() -> {
            try {
                return EncryptionUtils.decryptVector(ciphertext, iv, key);
            } catch (Exception e) {
                logger.error("Query decryption failed", e);
                throw new CryptoException("Query decryption failed", e);
            }
        });
    }

    @Override
    public byte[] encrypt(double[] vector, SecretKey key, byte[] iv) {
        return encryptTimer.record(() -> {
            try {
                return EncryptionUtils.encryptVector(vector, iv, key);
            } catch (Exception e) {
                logger.error("Encryption failed for vector", e);
                throw new CryptoException("Encryption failed", e);
            }
        });
    }

    @Override
    public EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey) {
        return encryptTimer.record(() -> {
            try {
                SecretKey oldKey = getOriginalKey(pt);
                double[] plaintext = EncryptionUtils.decryptVector(pt.getCiphertext(), pt.getIv(), oldKey);
                byte[] newIv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(plaintext, newIv, newKey);
                int newVersion = keyService.getCurrentVersion().getVersion();
                EncryptedPoint reEncrypted = new EncryptedPoint(pt.getId(), pt.getShardId(), newIv, ciphertext, newVersion);
                metadataManager.updateVectorMetadata(pt.getId(), Map.of("version", String.valueOf(newVersion)));
                return reEncrypted;
            } catch (Exception e) {
                logger.error("Re-encryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Re-encryption failed", e);
            }
        });
    }

    private SecretKey getOriginalKey(EncryptedPoint pt) {
        try {
            Map<String, String> meta = metadataManager.getVectorMetadata(pt.getId());
            String versionStr = meta.get("version");
            if (versionStr == null) {
                logger.warn("No version found for point {}, defaulting to current version", pt.getId());
                return keyService.getCurrentVersion().getKey();
            }
            int version = Integer.parseInt(versionStr);
            return keyService.getVersion(version).getKey();
        } catch (Exception e) {
            logger.error("Failed to retrieve original key for point {}", pt.getId(), e);
            throw new CryptoException("Failed to retrieve original key", e);
        }
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
}
