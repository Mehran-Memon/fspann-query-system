package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.KeyUtils;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialTest {

    @Test
    public void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempDir) throws Exception {
        System.out.println("========== Forward Security Test ==========");

        Path config = tempDir.resolve("config.json");
        Files.writeString(config, "{" +
                "\"numShards\":4," +
                "\"profilerEnabled\":true," +
                "\"opsThreshold\":999999," +
                "\"ageThresholdMs\":999999}");

        Path dummyData = tempDir.resolve("dummy.csv");
        Files.writeString(dummyData, "0.0,0.0,0.0\n");

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = Collections.singletonList(3);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());

        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                config.toString(),
                dummyData.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                true,
                metadataManager,
                cryptoService
        );

        // Ensure indexService is injected for re-encryption
        keyService.setIndexService(system.getIndexService());

        int dim = 3;
        double[] pointBefore = {0.15, 0.15, 0.15};
        String beforeId = UUID.randomUUID().toString();
        system.insert(beforeId, pointBefore, dim);

        SecretKey compromisedKey = KeyUtils.fromBytes(
                system.getCryptoService().getKeyService().getCurrentVersion().getKey().getEncoded()
        );
        int versionBefore = system.getCryptoService().getKeyService().getCurrentVersion().getVersion();
        System.out.println("Compromised key (base64): " + Base64.getEncoder().encodeToString(compromisedKey.getEncoded()));

        system.flush();

        KeyVersion rotated = keyService.rotateKey();
        keyService.reEncryptAll();

        int versionAfter = rotated.getVersion();
        System.out.printf("Key Version Before: %d, After: %d\n", versionBefore, versionAfter);
        assertTrue(versionAfter > versionBefore, "Key should have rotated");

        double[] pointAfter = {0.25, 0.25, 0.25};
        String afterId = UUID.randomUUID().toString();
        int currentVersion = system.getCryptoService().getKeyService().getCurrentVersion().getVersion();
        System.out.printf("CryptoService current version before post-rotation insert: %d%n", currentVersion);
        assertEquals(versionAfter, currentVersion, "CryptoService should reflect rotated key version");

        system.insert(afterId, pointAfter, dim);
        system.flush();

        EncryptedPoint encryptedAfter = system.getEncryptedPointById(afterId);
        assertNotNull(encryptedAfter, "Post-rotation point should exist.");
        assertEquals(versionAfter, encryptedAfter.getVersion(), "Point version should match current key version after rotation.");

        List<QueryResult> results = system.query(pointBefore, 20, dim);
        boolean matchFound = results.stream().anyMatch(r -> r.getId().equals(beforeId));
        System.out.println("Query results:");
        results.forEach(r -> System.out.printf("Returned ID: %s | Distance: %.6f\n", r.getId(), r.getDistance()));
        assertTrue(matchFound, "Pre-compromise point should be found in the query results.");

        EncryptedPoint encryptedBefore = system.getEncryptedPointById(beforeId);
        assertNotNull(encryptedBefore, "Pre-rotation point should exist.");
        Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(encryptedBefore, compromisedKey);
        System.out.println("Decryption of pre-rotation point with compromised key: " + (decryptedOld.isEmpty() ? "BLOCKED" : "FAILED"));
        assertTrue(decryptedOld.isEmpty(), "Old key should NOT decrypt re-encrypted point after key rotation");

        SecretKey currentKey = keyService.getVersion(encryptedAfter.getVersion()).getKey();
        Optional<double[]> decryptedNew = KeyUtils.tryDecryptWithKeyOnly(encryptedAfter, currentKey);
        System.out.println("Decryption of post-rotation point with current key: " + (decryptedNew.isPresent() ? "SUCCESS" : "FAILED"));
        assertTrue(decryptedNew.isPresent(), "Current key should decrypt post-rotation point");

        System.out.println("========= FORWARD SECURITY VALIDATED =========");
        system.shutdown();
    }
}
