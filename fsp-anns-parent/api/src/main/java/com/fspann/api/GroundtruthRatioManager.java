package com.fspann.api;

import com.fspann.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;

public final class GroundtruthRatioManager {
    private static final Logger log = LoggerFactory.getLogger(GroundtruthRatioManager.class);

    public static Path resolveOrCompute(SystemConfig.RatioConfig cfg,
                                        Path baseFvecsOrBvecs,
                                        Path queryFvecsOrBvecs,
                                        Integer k,
                                        Integer threads) throws Exception {
        // 1) Prefer explicit path
        if (cfg.gtPath != null && !cfg.gtPath.isBlank()) {
            Path gt = Paths.get(cfg.gtPath).toAbsolutePath().normalize();
            if (Files.isRegularFile(gt)) {
                log.info("Using existing groundtruth at {}", gt);
                return gt;
            }
            if (!cfg.allowComputeIfMissing) {
                throw new IllegalStateException("Groundtruth missing at " + gt + " and allowComputeIfMissing=false");
            }
            if (!cfg.autoComputeGT) {
                throw new IllegalStateException("Groundtruth missing and autoComputeGT=false (no recompute allowed)");
            }
            log.warn("Groundtruth not found at {}, computing due to allowComputeIfMissing=true & autoComputeGT=true", gt);
            // Compute exactly to requested path
            Files.createDirectories(gt.getParent());
            return GroundtruthPrecompute.run(baseFvecsOrBvecs, queryFvecsOrBvecs, gt, k != null ? k : 100, threads != null ? threads : Math.max(1, Runtime.getRuntime().availableProcessors()/2));
        }

        // 2) No explicit path: either compute (if allowed) or use default location
        Path defaultOut = GroundtruthPrecompute.defaultOutputForQuery(queryFvecsOrBvecs);
        if (Files.isRegularFile(defaultOut)) {
            log.info("Using existing groundtruth at {}", defaultOut);
            return defaultOut;
        }
        if (!cfg.allowComputeIfMissing) {
            throw new IllegalStateException("Groundtruth missing at default location " + defaultOut + " and allowComputeIfMissing=false");
        }
        if (!cfg.autoComputeGT) {
            throw new IllegalStateException("Groundtruth missing and autoComputeGT=false (no recompute allowed)");
        }
        log.warn("Groundtruth not found at default {}; computing due to flags", defaultOut);
        return GroundtruthPrecompute.run(baseFvecsOrBvecs, queryFvecsOrBvecs, defaultOut, k != null ? k : 100, threads != null ? threads : Math.max(1, Runtime.getRuntime().availableProcessors()/2));
    }

    private GroundtruthRatioManager() {}
}
