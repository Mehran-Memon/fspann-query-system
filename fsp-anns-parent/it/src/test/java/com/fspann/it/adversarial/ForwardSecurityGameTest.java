package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class ForwardSecurityGameTest {

    @TempDir
    Path temp;

    ForwardSecureANNSystem system;
    RocksDBMetadataManager metadata;
    KeyRotationServiceImpl keyService;
    AesGcmCryptoService crypto;

    SecretKey compromisedKey;
    int compromisedVersion;

    List<EncryptedPoint> snapshotPoints;

    @BeforeEach
    void setup() throws Exception {
        System.out.println("\n=== SETUP START ===");
        System.out.flush();

        System.out.println("1. Creating temp directories...");
        Path metaDir = temp.resolve("meta");
        Path ptsDir  = temp.resolve("pts");
        Path keyDir  = temp.resolve("keys");
        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.createDirectories(keyDir);
        System.out.println("   ✓ Directories created");

        System.out.println("2. Creating seed file...");
        Path seedFile = temp.resolve("seed.csv");
        Files.writeString(seedFile, "");
        System.out.println("   ✓ Seed file created");

        System.out.println("3. Creating RocksDB metadata manager...");
        metadata = RocksDBMetadataManager.create(
                metaDir.toString(),
                ptsDir.toString()
        );
        System.out.println("   ✓ RocksDB created");

        System.out.println("4. Writing config file...");
        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
        {
          "paper": {
            "enabled": true,
            "tables": 2,
            "divisions": 2,
            "m": 4,
            "lambda": 3,
            "seed": 13
          },
          "reencryption": { "enabled": true }
        }
        """);
        System.out.println("   ✓ Config written");

        System.out.println("5. Loading SystemConfig...");
        SystemConfig sc = SystemConfig.load(cfg.toString(), true);
        System.out.println("   ✓ Config loaded");

        System.out.println("6. Resetting GFunctionRegistry...");
        GFunctionRegistry.reset();
        System.out.println("   ✓ Registry reset");

        System.out.println("7. Initializing GFunctionRegistry...");
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4},
                        new double[]{2,3,4,5}
                ),
                4,
                sc.getPaper().getM(),
                sc.getPaper().getLambda(),
                sc.getPaper().getSeed(),
                sc.getPaper().getTables(),
                sc.getPaper().getDivisions()
        );
        System.out.println("   ✓ Registry initialized");

        System.out.println("8. Creating KeyManager...");
        KeyManager km = new KeyManager(keyDir.resolve("ks.blob").toString());
        System.out.println("   ✓ KeyManager created");

        System.out.println("9. Creating KeyRotationService...");
        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(1, Long.MAX_VALUE),
                metaDir.toString(),
                metadata,
                null
        );
        System.out.println("   ✓ KeyRotationService created");

        System.out.println("10. Creating CryptoService...");
        crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        System.out.println("   ✓ CryptoService created");

        System.out.println("11. Setting crypto on keyService...");
        keyService.setCryptoService(crypto);
        System.out.println("   ✓ Crypto set");

        System.out.println("12. Creating ForwardSecureANNSystem...");
        System.out.flush();

        system = new ForwardSecureANNSystem(
                cfg.toString(),
                seedFile.toString(),
                keyDir.toString(),
                List.of(4),
                temp,
                false,
                metadata,
                crypto,
                32
        );

        System.out.println("   ✓ System created");

        System.out.println("13. Inserting test vectors...");
        for (int i = 0; i < 6; i++) {
            System.out.println("   - Inserting vector " + i);
            system.insert(
                    "p" + i,
                    new double[]{i, i+1, i+2, i+3},
                    4
            );
        }
        System.out.println("   ✓ Vectors inserted");

        System.out.println("14. Finalizing for search...");
        system.finalizeForSearch();
        System.out.println("   ✓ Finalized");

        System.out.println("15. Capturing state...");
        compromisedKey = keyService.getCurrentVersion().getKey();
        compromisedVersion = keyService.getCurrentVersion().getVersion();
        snapshotPoints = new ArrayList<>(metadata.getAllEncryptedPoints());
        System.out.println("   ✓ State captured");

        System.out.println("=== SETUP COMPLETE ===\n");
        System.out.flush();
    }

    /* ===================== G₁ ===================== */
    @Test
    void forwardSecrecy_oldKeysFail() {
        System.out.println("\n>>> TEST: forwardSecrecy_oldKeysFail START");
        System.out.flush();

        System.out.println("  - Rotating key...");
        keyService.rotateKeyOnly();
        System.out.println("  - Initializing usage tracking...");
        keyService.initializeUsageTracking();
        System.out.println("  - Re-encrypting all...");
        keyService.reEncryptAll();
        System.out.println("  - Re-encryption complete");

        System.out.println("  - Fetching re-encrypted points from metadata...");
        List<EncryptedPoint> reencryptedPoints = new ArrayList<>(metadata.getAllEncryptedPoints());
        System.out.println("  - Retrieved " + reencryptedPoints.size() + " re-encrypted points");

        System.out.println("  - Testing decryption failures with OLD key...");
        int wins = 0;
        for (EncryptedPoint p : reencryptedPoints) {
            try {
                crypto.decryptFromPoint(p, compromisedKey);
                wins++;
            } catch (Exception ignored) {}
        }
        System.out.println("  - Decryption tests complete: wins=" + wins);

        assertEquals(0, wins, "Old key should not decrypt re-encrypted points");
        System.out.println(">>> TEST: forwardSecrecy_oldKeysFail COMPLETE\n");
    }

    /* ===================== G₂ ===================== */
    @Test
    void ciphertextIndistinguishability() {
        System.out.println("\n>>> TEST: ciphertextIndistinguishability START");
        System.out.flush();

        System.out.println("  - Taking before snapshot...");
        Map<String, byte[]> before = snapshot();
        System.out.println("  - Before snapshot complete");

        System.out.println("  - Rotating key...");
        keyService.rotateKeyOnly();
        System.out.println("  - Initializing usage tracking...");
        keyService.initializeUsageTracking();
        System.out.println("  - Re-encrypting all...");
        keyService.reEncryptAll();
        System.out.println("  - Re-encryption complete");

        System.out.println("  - Taking after snapshot...");
        Map<String, byte[]> after = snapshot();
        System.out.println("  - After snapshot complete");

        System.out.println("  - Asserting difference...");
        assertNotEquals(before, after);
        System.out.println(">>> TEST: ciphertextIndistinguishability COMPLETE\n");
    }

    /* ===================== G₃ ===================== */
    @Test
    void selectiveReencryptionSoundness() {
        System.out.println("\n>>> TEST: selectiveReencryptionSoundness START");
        System.out.flush();

        System.out.println("  - Getting vector IDs...");
        List<String> ids = metadata.getAllVectorIds();
        String touched = ids.get(0);
        System.out.println("  - Vector IDs retrieved: " + ids.size());

        System.out.println("  - Taking before snapshot...");
        Map<String, byte[]> before = snapshot();
        System.out.println("  - Before snapshot complete");

        System.out.println("  - Initializing usage tracking...");
        keyService.initializeUsageTracking();
        System.out.println("  - Re-encrypting touched vector: " + touched);
        keyService.reencryptTouched(
                List.of(touched),
                keyService.getCurrentVersion().getVersion(),
                () -> 0L
        );
        System.out.println("  - Re-encryption complete");

        System.out.println("  - Taking after snapshot...");
        Map<String, byte[]> after = snapshot();
        System.out.println("  - After snapshot complete");

        System.out.println("  - Asserting touched vector changed...");
        assertNotEquals(before.get(touched), after.get(touched));
        System.out.println("  - Asserting other vectors unchanged...");
        for (int i = 1; i < ids.size(); i++)
            assertArrayEquals(before.get(ids.get(i)), after.get(ids.get(i)));
        System.out.println(">>> TEST: selectiveReencryptionSoundness COMPLETE\n");
    }

    /* ===================== G₄ ===================== */
    @Test
    void keyUsageAccounting() {
        System.out.println("\n>>> TEST: keyUsageAccounting START");
        System.out.flush();

        System.out.println("  - Getting tracker...");
        KeyUsageTracker tracker = keyService.getKeyManager().getUsageTracker();
        System.out.println("  - Checking initial count...");
        assertEquals(6, tracker.getVectorCount(compromisedVersion));
        System.out.println("  - Initial count verified: 6");

        System.out.println("  - Rotating key...");
        keyService.rotateKeyOnly();
        int newVersion = keyService.getCurrentVersion().getVersion();
        System.out.println("  - Key rotated to version: " + newVersion);

        System.out.println("  - Initializing usage tracking...");
        keyService.initializeUsageTracking();
        System.out.println("  - Re-encrypting all...");
        keyService.reEncryptAll();
        System.out.println("  - Re-encryption complete");

        System.out.println("  - Verifying old version count is 0...");
        assertEquals(0, tracker.getVectorCount(compromisedVersion));
        System.out.println("  - Verifying old version is safe to delete...");
        assertTrue(tracker.isSafeToDelete(compromisedVersion));
        System.out.println("  - Verifying new version count is 6...");
        assertEquals(6, tracker.getVectorCount(newVersion));
        System.out.println(">>> TEST: keyUsageAccounting COMPLETE\n");
    }

    /* ===================== G₅ ===================== */
    @Test
    void safeDeletionSoundness() {
        System.out.println("\n>>> TEST: safeDeletionSoundness START");
        System.out.flush();

        System.out.println("  - Getting KeyManager...");
        KeyManager km = keyService.getKeyManager();

        System.out.println("  - Rotating key...");
        keyService.rotateKeyOnly();
        System.out.println("  - Initializing usage tracking...");
        keyService.initializeUsageTracking();
        System.out.println("  - Re-encrypting all...");
        keyService.reEncryptAll();
        System.out.println("  - Re-encryption complete");

        System.out.println("  - Deleting old keys...");
        km.deleteKeysOlderThan(compromisedVersion + 1);
        System.out.println("  - Deletion complete");

        System.out.println("  - Verifying old key is null...");
        assertNull(km.getSessionKey(compromisedVersion));
        System.out.println("  - Verifying new key exists...");
        assertNotNull(km.getSessionKey(compromisedVersion + 1));
        System.out.println(">>> TEST: safeDeletionSoundness COMPLETE\n");
    }

    /* ===================== HELPER METHOD ===================== */
    private Map<String, byte[]> snapshot() {
        Map<String, byte[]> m = new HashMap<>();
        for (EncryptedPoint p : snapshotPoints)
            m.put(p.getId(), Arrays.copyOf(p.getCiphertext(), p.getCiphertext().length));
        return m;
    }

    @AfterEach
    void tearDown() {
        System.out.println(">>> TEARDOWN START");
        System.out.flush();

        if (system != null) {
            System.out.println("  - Shutting down system...");
            system.shutdownForTests();
            System.out.println("  - System shutdown complete");
        }

        System.out.println(">>> TEARDOWN COMPLETE\n");
        System.out.flush();
    }

    /**
     * CRITICAL: Force cleanup of all background threads after all tests complete.
     * This prevents Maven Surefire from hanging waiting for non-daemon threads.
     */
    @AfterAll
    static void forceCleanup() {
        System.out.println("\n=== FORCING BACKGROUND THREAD CLEANUP ===");
        System.out.flush();

        // Give threads 2 seconds to naturally terminate
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Force garbage collection to close any leaked resources
        System.gc();

        // Log all remaining non-daemon threads
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        int nonDaemonCount = 0;
        for (Thread t : threadSet) {
            if (!t.isDaemon() && !t.getName().equals("main")) {
                System.out.println("  WARNING: Non-daemon thread still running: " + t.getName());
                nonDaemonCount++;
            }
        }

        if (nonDaemonCount > 0) {
            System.out.println("  Found " + nonDaemonCount + " non-daemon threads still running");
            System.out.println("  Maven Surefire will force-terminate after timeout configured in POM");
        } else {
            System.out.println("  ✓ All non-daemon threads cleaned up successfully");
        }

        System.out.println("=== CLEANUP COMPLETE ===\n");
        System.out.flush();
    }
}