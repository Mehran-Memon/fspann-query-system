package com.fspann.crypto;

import com.fspann.common.EncryptedPoint;
import javax.crypto.SecretKey;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class AesGcmCryptoService implements CryptoService {
    private final MeterRegistry registry;
    private final Timer encryptTimer;
    private final Timer decryptTimer;

    /**
     * default no-arg: uses a simple in-memory registry
     */
    public AesGcmCryptoService() {
        this(new SimpleMeterRegistry());
    }

    /**
     * primary ctor: use your PrometheusMeterRegistry
     */
    public AesGcmCryptoService(MeterRegistry registry) {
        this.registry = registry;
        this.encryptTimer = Timer
                .builder("fspann.crypto.encrypt.time")
                .description("AES-GCM encryption latency")
                .register(registry);
        this.decryptTimer = Timer
                .builder("fspann.crypto.decrypt.time")
                .description("AES-GCM decryption latency")
                .register(registry);
    }

    @Override
    public EncryptedPoint encryptToPoint(String id, double[] vector, SecretKey key) {
        try {
            byte[] iv = EncryptionUtils.generateIV();
            byte[] ct = EncryptionUtils.encryptVector(vector, iv, key);  // might throw
            // “0” for shard here; real code will overwrite that when you index
            return new EncryptedPoint(id, 0, iv, ct);
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed for point " + id, e);
        }
    }

    @Override
    public double[] decryptFromPoint(EncryptedPoint p, SecretKey key) {
        try {
            return EncryptionUtils.decryptVector(p.getCiphertext(), p.getIv(), key);  // might throw
        } catch (Exception e) {
            throw new RuntimeException("Decryption failed for point " + p.getId(), e);
        }
    }

    @Override
    public double[] decryptQuery(byte[] ciphertext, byte[] iv, SecretKey key) {
        try {
            return EncryptionUtils.decryptVector(ciphertext, iv, key);
        } catch (Exception e) {
            throw new RuntimeException("Query decryption failed", e);
        }
    }
}
