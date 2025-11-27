package com.fspann.key;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background re-encryption scheduler that minimizes query latency impact.
 *
 * Features:
 * - Runs in separate thread pool (doesn't block queries)
 * - Rate-limited to avoid I/O saturation
 * - Batched updates to amortize overhead
 * - Skips re-encryption if query load is high
 */
public class BackgroundReencryptionScheduler {
    private static final Logger logger = LoggerFactory.getLogger(
            BackgroundReencryptionScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final KeyRotationServiceImpl keyService;
    private final CryptoService cryptoService;
    private final IndexService indexService;
    private final RocksDBMetadataManager metadataManager;

    // Rate limiting: max points/sec to re-encrypt
    private final int maxReencryptRatePerSec;

    // Query load threshold: pause if query rate exceeds this
    private final int queryLoadThreshold;
    private volatile int recentQueryCount = 0;

    // Stats
    private final AtomicInteger totalReencrypted = new AtomicInteger(0);

    public BackgroundReencryptionScheduler(
            KeyRotationServiceImpl keyService,
            CryptoService cryptoService,
            IndexService indexService,
            RocksDBMetadataManager metadataManager
    ) {
        this.keyService = keyService;
        this.cryptoService = cryptoService;
        this.indexService = indexService;
        this.metadataManager = metadataManager;

        this.maxReencryptRatePerSec = Integer.getInteger(
                "reenc.background.rateLimit", 1000);
        this.queryLoadThreshold = Integer.getInteger(
                "reenc.background.queryThreshold", 50);

        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "background-reencrypt");
            t.setDaemon(true);
            t.setPriority(Thread.MIN_PRIORITY); // Low priority
            return t;
        });

        logger.info("BackgroundReencryptionScheduler initialized: " +
                        "rateLimit={}/sec, queryThreshold={}",
                maxReencryptRatePerSec, queryLoadThreshold);
    }

    /**
     * Start periodic background re-encryption.
     *
     * @param intervalMinutes How often to check for re-encryption candidates
     */
    public void start(int intervalMinutes) {
        scheduler.scheduleAtFixedRate(
                this::reencryptCycle,
                intervalMinutes,
                intervalMinutes,
                TimeUnit.MINUTES
        );

        logger.info("Background re-encryption started: interval={}min",
                intervalMinutes);
    }

    /**
     * Called from query thread to track load.
     */
    public void recordQuery() {
        recentQueryCount++;
    }

    /**
     * One re-encryption cycle (non-blocking).
     */
    private void reencryptCycle() {
        try {
            // Check query load: skip if system is busy
            int qps = recentQueryCount; // snapshot
            recentQueryCount = 0; // reset counter

            if (qps > queryLoadThreshold) {
                logger.debug("Skipping re-encryption cycle: high query load (qps={})",
                        qps);
                return;
            }

            // Get current key version
            int targetVersion = keyService.getCurrentVersion().getVersion();

            // Find candidates needing re-encryption
            List<String> candidates = findReencryptionCandidates(targetVersion);

            if (candidates.isEmpty()) {
                logger.debug("No re-encryption candidates found");
                return;
            }

            logger.info("Starting background re-encryption: {} candidates, " +
                    "targetVersion=v{}", candidates.size(), targetVersion);

            int reencrypted = reencryptBatch(candidates, targetVersion);
            totalReencrypted.addAndGet(reencrypted);

            logger.info("Background re-encryption complete: {}/{} points, " +
                            "totalReencrypted={}",
                    reencrypted, candidates.size(), totalReencrypted.get());

        } catch (Exception e) {
            logger.error("Background re-encryption cycle failed", e);
        }
    }

    /**
     * Find points needing re-encryption (old versions).
     */
    private List<String> findReencryptionCandidates(int targetVersion) {
        List<String> candidates = new ArrayList<>();

        // Sample metadata to find old versions (avoid full scan)
        List<String> allIds = metadataManager.getAllVectorIds();
        int sampleSize = Math.min(1000, allIds.size()); // Sample only
        Collections.shuffle(allIds);

        for (int i = 0; i < sampleSize; i++) {
            String id = allIds.get(i);
            Map<String, String> meta = metadataManager.getVectorMetadata(id);

            if (meta.isEmpty() || !meta.containsKey("version")) {
                continue;
            }

            try {
                int version = Integer.parseInt(meta.get("version"));
                if (version < targetVersion) {
                    candidates.add(id);
                }
            } catch (NumberFormatException ignore) {}
        }

        return candidates;
    }

    /**
     * Re-encrypt a batch with rate limiting.
     */
    private int reencryptBatch(List<String> ids, int targetVersion) {
        int reencrypted = 0;
        long startTime = System.currentTimeMillis();

        for (String id : ids) {
            try {
                // Rate limiting: sleep if we're going too fast
                if (reencrypted > 0 && reencrypted % 100 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    long expectedTime = (reencrypted * 1000L) / maxReencryptRatePerSec;

                    if (elapsed < expectedTime) {
                        Thread.sleep(expectedTime - elapsed);
                    }
                }

                // Load point
                EncryptedPoint pt = metadataManager.loadEncryptedPoint(id);
                if (pt == null || pt.getVersion() >= targetVersion) {
                    continue;
                }

                // Decrypt with old key
                KeyVersion oldVer = keyService.getVersion(pt.getVersion());
                double[] plaintext = cryptoService.decryptFromPoint(pt, oldVer.getKey());

                // Re-encrypt with current key
                EncryptedPoint updated = cryptoService.encrypt(id, plaintext);

                // Persist
                metadataManager.saveEncryptedPoint(updated);
                indexService.updateCachedPoint(updated);

                reencrypted++;

            } catch (Exception e) {
                logger.debug("Failed to re-encrypt point {}", id, e);
            }
        }

        return reencrypted;
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        logger.info("BackgroundReencryptionScheduler stopped");
    }

    public int getTotalReencrypted() {
        return totalReencrypted.get();
    }
}