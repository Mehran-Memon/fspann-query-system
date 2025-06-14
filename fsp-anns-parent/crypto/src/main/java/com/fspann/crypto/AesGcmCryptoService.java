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

public class AesGcmCryptoService implements CryptoService {
    private static final Logger logger = LoggerFactory.getLogger(AesGcmCryptoService.class);
    private final MeterRegistry registry;
    private final Timer encryptTimer;
    private final Timer decryptTimer;
    private final KeyLifeCycleService keyService;
    private final MetadataManager metadataManager;

    /**
     * Default no-arg constructor: uses a simple in-memory registry.
     */
    public AesGcmCryptoService() {
        this(new SimpleMeterRegistry(), null, null); // Placeholder for keyService and metadataManager
    }

    /**
     * Primary constructor: use your PrometheusMeterRegistry with key service and metadata.
     */
    public AesGcmCryptoService(MeterRegistry registry, KeyLifeCycleService keyService, MetadataManager metadataManager) {
        this.registry = registry != null ? registry : new SimpleMeterRegistry();
        this.keyService = keyService;
        this.metadataManager = metadataManager != null ? metadataManager : new MetadataManager();
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
                byte[] ct = EncryptionUtils.encryptVector(vector, iv, key);
                return new EncryptedPoint(id, 0, iv, ct, 1); // Added version argument
            } catch (Exception e) {
                logger.error("Encryption failed for point {}", id, e);
                throw new CryptoException("Encryption failed for point " + id, e);
            }
        });
    }

    @Override
    public double[] decryptFromPoint(EncryptedPoint p, SecretKey key) {
        try {
            return EncryptionUtils.decryptVector(p.getCiphertext(), p.getIv(), key);
        } catch (Exception e) {
            logger.error("Decryption failed for point {}", p.getId(), e);
            throw new CryptoException("Decryption failed for point " + p.getId(), e);
        }
    }

    @Override
    public double[] decryptQuery(byte[] ciphertext, byte[] iv, SecretKey key) {
        try {
            return EncryptionUtils.decryptVector(ciphertext, iv, key);
        } catch (Exception e) {
            logger.error("Query decryption failed", e);
            throw new CryptoException("Query decryption failed", e);
        }
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

    public static class CryptoException extends RuntimeException {
        public CryptoException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Override
    public EncryptedPoint reEncrypt(EncryptedPoint pt, SecretKey newKey) {
        return encryptTimer.record(() -> {
            try {
                // Decrypt with the original key retrieved from metadata
                double[] plaintext = decryptFromPoint(pt, getOriginalKey(pt));
                byte[] iv = EncryptionUtils.generateIV();
                byte[] ciphertext = EncryptionUtils.encryptVector(plaintext, iv, newKey);
                return new EncryptedPoint(pt.getId(), pt.getShardId(), iv, ciphertext, pt.getVersion());
            } catch (Exception e) {
                logger.error("Re-encryption failed for point {}", pt.getId(), e);
                throw new CryptoException("Re-encryption failed", e);
            }
        });
    }

    private SecretKey getOriginalKey(EncryptedPoint pt) {
        if (keyService == null || metadataManager == null) {
            throw new CryptoException("KeyService or MetadataManager not initialized", null);
        }
        try {
            Map<String, String> meta = metadataManager.getVectorMetadata(pt.getId());
            String versionStr = meta.get("version");
            if (versionStr == null) {
                throw new CryptoException("No version found for point " + pt.getId(), null);
            }
            int version = Integer.parseInt(versionStr);
            return keyService.getVersion(version).getSecretKey(); // Changed to getVersion
        } catch (Exception e) {
            logger.error("Failed to retrieve original key for point {}", pt.getId(), e);
            throw new CryptoException("Failed to retrieve original key", e);
        }
    }

    @Override
    public byte[] generateIV() {
        return EncryptionUtils.generateIV(); // Delegate to utility
    }
}