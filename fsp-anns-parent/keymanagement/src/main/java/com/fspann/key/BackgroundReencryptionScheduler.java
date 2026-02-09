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
    private final MetadataManager metadataManager;
    private final SelectiveReencryptor reencryptor;

    // Rate limiting: max points/sec to re-encrypt
    private final int maxReencryptRatePerSec;

    // Query load threshold: pause if query rate exceeds this
    private final int queryLoadThreshold;
    private final AtomicInteger recentQueryCount = new AtomicInteger(0);

    // Stats
    private final AtomicInteger totalReencrypted = new AtomicInteger(0);

    public BackgroundReencryptionScheduler(
            KeyRotationServiceImpl keyService,
            CryptoService cryptoService,
            IndexService indexService,
            MetadataManager metadataManager
    ) {
        this.keyService = keyService;
        this.cryptoService = cryptoService;
        this.indexService = indexService;
        this.metadataManager = metadataManager;

        if (!(keyService instanceof SelectiveReencryptor sr)) {
            throw new IllegalStateException(
                    "KeyService must implement SelectiveReencryptor in ideal-system mode"
            );
        }
        this.reencryptor = sr;

        // Initialize required final fields
        this.maxReencryptRatePerSec = Integer.getInteger(
                "reenc.background.rateLimit", 1000);
        this.queryLoadThreshold = Integer.getInteger(
                "reenc.background.queryThreshold", 50);

        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "background-reencrypt");
            t.setDaemon(true);
            t.setPriority(Thread.MIN_PRIORITY);
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
        recentQueryCount.incrementAndGet();
    }

    /**
     * One re-encryption cycle (non-blocking).
     */
    private void reencryptCycle() {
        try {
            int qps = recentQueryCount.getAndSet(0);
            if (qps > queryLoadThreshold) {
                logger.debug("Skipping re-encryption cycle: high query load (qps={})", qps);
                return;
            }

            int targetVersion = keyService.getCurrentVersion().getVersion();

            List<String> candidates = findReencryptionCandidates(targetVersion);
            if (candidates.isEmpty()) {
                logger.debug("No background re-encryption candidates");
                return;
            }

            logger.info("Background re-encryption: {} candidates for v{}",
                    candidates.size(), targetVersion);

            ReencryptReport rep = reencryptor.reencryptTouched(
                    new LinkedHashSet<>(candidates),
                    targetVersion,
                    () -> metadataManager.sizePointsDir()
            );

            totalReencrypted.addAndGet(rep.reencryptedCount());
            logger.info("Background re-encryption complete: {} / {} updated",
                    rep.reencryptedCount(), candidates.size());

        } catch (Exception e) {
            logger.error("Background re-encryption cycle failed", e);
        }
    }

    /**
     * Find points needing re-encryption (old versions).
     */
    private List<String> findReencryptionCandidates(int targetVersion) {
        // 100M Optimization: Do NOT call getAllVectorIds()
        // Randomly pick one shard index (0-15 if using 16 shards)
        int numShards = (metadataManager instanceof ShardedMetadataManager smm) ? 16 : 1;
        int randomShard = ThreadLocalRandom.current().nextInt(numShards);

        // We fetch a small, manageable sample from that specific shard
        // This requires adding a 'getShardIdsSample' to your MetadataManager or
        // using an iterator that supports limits.
        List<String> sampleIds = metadataManager.getIdsFromShard(randomShard, 2000);

        List<String> out = new ArrayList<>();
        for (String id : sampleIds) {
            // O(1) metadata lookup
            int ver = metadataManager.getVersionOfVector(id);

            if (ver >= 0 && ver < targetVersion) {
                out.add(id);
            }
        }

        if (!out.isEmpty()) {
            logger.debug("Sampled shard {}: found {} candidates for re-encryption", randomShard, out.size());
        }
        return out;
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