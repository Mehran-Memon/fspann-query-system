package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Canonical system configuration for FSP-ANN.
 *
 * - Loaded from JSON via {@link #load(String, boolean)}.
 * - Cached per absolute/real path.
 * - Exposes nested config blocks: LSH, paper, reencryption, eval, output, audit, cloak, ratio, kAware.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SystemConfig {

    private static final int  MAX_SHARDS        = 8192;
    private static final int  MAX_TABLES        = 1024;
    private static final long MAX_OPS_THRESHOLD = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD = 365L * 24L * 60L * 60L * 1000L; // 1 year

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Per-path cache for loaded configs. */
    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();

    /* ======================== Top-level fields ======================== */

    @JsonProperty("numShards")
    private int numShards = 32;

    @JsonProperty("numTables")
    private int numTables = 8;

    @JsonProperty("opsThreshold")
    private long opsThreshold = 500_000_000L;

    @JsonProperty("ageThresholdMs")
    private long ageThresholdMs = 24L * 60L * 60L * 1000L; // 1 day

    @JsonProperty("reencryptionEnabled")
    private boolean reencryptionEnabled = true;

    @JsonProperty("forwardSecurityEnabled")
    private boolean forwardSecurityEnabled = true;

    @JsonProperty("partitionedIndexingEnabled")
    private boolean partitionedIndexingEnabled = true;

    /** Whether MicrometerProfiler should be enabled. */
    @JsonProperty("profilerEnabled")
    private boolean profilerEnabled = true;

    @JsonProperty("lsh")
    private LshConfig lsh = new LshConfig();

    @JsonProperty("reencryption")
    private ReencryptionConfig reencryption = new ReencryptionConfig();

    @JsonProperty("paper")
    private PaperConfig paper = new PaperConfig();

    @JsonProperty("eval")
    private EvalConfig eval = new EvalConfig();

    @JsonProperty("output")
    private OutputConfig output = new OutputConfig();

    @JsonProperty("audit")
    private AuditConfig audit = new AuditConfig();

    @JsonProperty("cloak")
    private CloakConfig cloak = new CloakConfig();

    @JsonProperty("ratio")
    private RatioConfig ratio = new RatioConfig();

    @JsonProperty("kAware")
    private KAwareConfig kAware = new KAwareConfig();

    @JsonProperty("stabilization")
    private StabilizationConfig stabilization = new StabilizationConfig();
    public StabilizationConfig getStabilization() { return stabilization; }

    /* ======================== Static loading API ======================== */

    public static SystemConfig load(String path, boolean refresh) throws ConfigLoadException {
        Objects.requireNonNull(path, "Config path cannot be null");
        String key;
        try {
            Path p = Paths.get(path).toAbsolutePath().normalize();
            try {
                p = p.toRealPath();
            } catch (IOException ignore) {
                // fall back to normalized absolute path
            }
            key = p.toString();
        } catch (Exception e) {
            throw new ConfigLoadException("Invalid config path: " + path, e);
        }

        if (!refresh) {
            SystemConfig cached = configCache.get(key);
            if (cached != null) return cached;
        }

        SystemConfig cfg;
        try {
            Path p = Paths.get(key);
            if (!Files.isRegularFile(p) || !Files.isReadable(p)) {
                throw new IOException("Config file not found or not readable: " + key);
            }
            cfg = MAPPER.readValue(p.toFile(), SystemConfig.class);
        } catch (IOException e) {
            throw new ConfigLoadException("Failed to read/parse SystemConfig from " + key, e);
        }

        cfg.numShards      = clamp(cfg.numShards, 1, MAX_SHARDS);
        cfg.numTables      = clamp(cfg.numTables, 1, MAX_TABLES);
        cfg.opsThreshold   = clamp(cfg.opsThreshold, 1L, MAX_OPS_THRESHOLD);
        cfg.ageThresholdMs = clamp(cfg.ageThresholdMs, 0L, MAX_AGE_THRESHOLD);

        configCache.put(key, cfg);
        return cfg;
    }

    public static void clearCache() {
        configCache.clear();
    }

    /* ======================== Getters used by other modules ======================== */

    public int getNumShards() {
        return clamp(numShards, 1, MAX_SHARDS);
    }

    public int getNumTables() {
        return clamp(numTables, 1, MAX_TABLES);
    }

    public long getOpsThreshold() {
        return clamp(opsThreshold, 1L, MAX_OPS_THRESHOLD);
    }

    public long getAgeThresholdMs() {
        return clamp(ageThresholdMs, 0L, MAX_AGE_THRESHOLD);
    }

    public boolean isForwardSecurityEnabled() {
        return forwardSecurityEnabled;
    }

    public boolean isPartitionedIndexingEnabled() {
        return partitionedIndexingEnabled;
    }

    public boolean isReencryptionEnabled() {
        return reencryptionEnabled;
    }

    /** Global re-encryption toggle used by ForwardSecureANNSystem. */
    public boolean isReencryptionGloballyEnabled() {
        return reencryptionEnabled && (reencryption == null || reencryption.isEnabled());
    }

    public boolean isProfilerEnabled() {
        return profilerEnabled;
    }

    public LshConfig getLsh() {
        return lsh;
    }

    public ReencryptionConfig getReencryption() {
        return reencryption;
    }

    public PaperConfig getPaper() {
        return paper;
    }

    public EvalConfig getEval() {
        return eval;
    }

    public OutputConfig getOutput() {
        return output;
    }

    public AuditConfig getAudit() {
        return audit;
    }

    public CloakConfig getCloak() {
        return cloak;
    }

    public RatioConfig getRatio() {
        return ratio;
    }

    public KAwareConfig getKAware() {
        return kAware;
    }

    /* ======================== Nested config types ======================== */

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LshConfig {
        @JsonProperty("numTables")
        public int numTables = 8;

        @JsonProperty("rowsPerBand")
        public int rowsPerBand = 0;

        @JsonProperty("probeShards")
        public int probeShards = 0;

        // ========== NEW FIELDS FOR LSH-ONLY SYSTEM ==========
        @JsonProperty("numFunctions")
        public int numFunctions = 8;

        @JsonProperty("numBuckets")
        public int numBuckets = 1000;

        @JsonProperty("adaptiveProbing")
        public boolean adaptiveProbing = true;

        @JsonProperty("targetRatio")
        public double targetRatio = 1.2;

        public int getNumTables() {
            return clamp(numTables, 1, MAX_TABLES);
        }

        public int getProbeShards() {
            if (probeShards <= 0) return 0;
            return clamp(probeShards, 1, MAX_SHARDS);
        }

        // ========== NEW GETTERS FOR LSH-ONLY ==========
        public int getNumFunctions() {
            return clamp(numFunctions, 1, 256);
        }

        public int getNumBuckets() {
            return clamp(numBuckets, 100, 1_000_000);
        }

        public boolean isAdaptiveProbing() {
            return adaptiveProbing;
        }

        public double getTargetRatio() {
            if (Double.isNaN(targetRatio) || targetRatio < 1.0) return 1.2;
            if (targetRatio > 2.0) return 2.0;
            return targetRatio;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StabilizationConfig {

        @JsonProperty("enabled")
        public boolean enabled = true;

        /**
         * Fraction of raw candidates to keep before minCandidates is applied.
         * Example: alpha = 0.10 means "keep 10% of raw candidates".
         */
        @JsonProperty("alpha")
        public double alpha = 0.10;

        /**
         * Hard lower bound on stabilized candidate count.
         * Ensures K=40...100 ratios stay stable and <1.3.
         */
        @JsonProperty("minCandidates")
        public int minCandidates = 1200;

        public boolean isEnabled() { return enabled; }

        public double getAlpha() {
            if (Double.isNaN(alpha) || alpha <= 0.0) return 0.01;
            if (alpha > 1.0) return 1.0;
            return alpha;
        }

        public int getMinCandidates() {
            return Math.max(1, minCandidates);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReencryptionConfig {
        @JsonProperty("enabled")
        public boolean enabled = true;

        @JsonProperty("batchSize")
        public int batchSize = 1024;

        @JsonProperty("maxMsPerBatch")
        public long maxMsPerBatch = 50L;

        public boolean isEnabled() {
            return enabled;
        }

        public int getBatchSize() {
            return Math.max(1, batchSize);
        }

        public long getMaxMsPerBatch() {
            return Math.max(0L, maxMsPerBatch);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PaperConfig {
        @JsonProperty("enabled")
        public boolean enabled = false;

        @JsonProperty("m")
        public int m = 12;

        @JsonProperty("lambda")
        public int lambda = 6;

        @JsonProperty("divisions")
        public int divisions = 8;

        @JsonProperty("seed")
        public long seed = 42L;

        @JsonProperty("safetyMaxCandidates")
        public int safetyMaxCandidates = 0;

        public boolean isEnabled() {
            return enabled;
        }

        public int getM() {
            return Math.max(1, m);
        }

        public int getLambda() {
            return Math.max(1, lambda);
        }

        public int getDivisions() {
            return Math.max(1, divisions);
        }

        public long getSeed() {
            return seed;
        }

        public int getSafetyMaxCandidates() {
            return Math.max(0, safetyMaxCandidates);
        }
    }

    /** Evaluation knobs (precision, K-variants, etc.). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EvalConfig {
        @JsonProperty("computePrecision")
        public boolean computePrecision = true;

        @JsonProperty("writeGlobalPrecisionCsv")
        public boolean writeGlobalPrecisionCsv = true;

        @JsonProperty("kVariants")
        public int[] kVariants = new int[]{1, 5, 10, 20, 40, 60, 80, 100};
    }

    /** Output / export behavior. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutputConfig {
        @JsonProperty("resultsDir")
        public String resultsDir = "results";

        @JsonProperty("suppressLegacyMetrics")
        public boolean suppressLegacyMetrics = false;

        @JsonProperty("exportArtifacts")
        public boolean exportArtifacts = true;
    }

    /** Audit sampling & worst-K logging. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuditConfig {
        @JsonProperty("enable")
        public boolean enable = false;

        @JsonProperty("k")
        public int k = 100;

        @JsonProperty("sampleEvery")
        public int sampleEvery = 100;

        @JsonProperty("worstKeep")
        public int worstKeep = 25;
    }

    /** Cloaked query parameters (noise scale, etc.). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CloakConfig {
        @JsonProperty("noise")
        public double noise = 0.0;
    }

    /** Ratio / groundtruth evaluation settings. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RatioConfig {
        /** "auto" | "gt" | "base". */
        @JsonProperty("source")
        public String source = "auto";

        /** Explicit GT path (optional). */
        @JsonProperty("gtPath")
        public String gtPath;

        /** If true, we may compute GT when missing. */
        @JsonProperty("allowComputeIfMissing")
        public boolean allowComputeIfMissing = false;

        /** If true and allowed, auto-compute GT when absent. */
        @JsonProperty("autoComputeGT")
        public boolean autoComputeGT = false;

        /** How many queries to sample when validating GT vs base. */
        @JsonProperty("gtSample")
        public int gtSample = 16;

        /** Allowed mismatch fraction between GT@1 and base true NN. */
        @JsonProperty("gtMismatchTolerance")
        public double gtMismatchTolerance = 0.10;
    }

    /** Optional K-aware candidate selection options. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KAwareConfig {
        @JsonProperty("enabled")
        public boolean enabled = false;

        @JsonProperty("baseOversample")
        public double baseOversample = 2.0;

        @JsonProperty("logFactor")
        public double logFactor = 0.5;

        @JsonProperty("minPerDiv")
        public int minPerDiv = 1;

        @JsonProperty("maxPerDiv")
        public int maxPerDiv = 0;
    }

    /* ======================== Helper methods ======================== */

    private static int clamp(int v, int min, int max) {
        if (v < min) return min;
        if (v > max) return max;
        return v;
    }

    private static long clamp(long v, long min, long max) {
        if (v < min) return min;
        if (v > max) return max;
        return v;
    }
    @JsonProperty("kAdaptive")
    private KAdaptiveConfig kAdaptive = new KAdaptiveConfig();
    public KAdaptiveConfig getKAdaptive() { return kAdaptive; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KAdaptiveConfig {
        @JsonProperty("enabled")
        public boolean enabled = false;

        // maximum times to widen for a single query
        @JsonProperty("maxRounds")
        public int maxRounds = 3;

        // target minimum return-rate at Kmax (e.g. 0.8 → 80%)
        @JsonProperty("targetReturnRate")
        public double targetReturnRate = 0.80;

        // guard to avoid insane work
        @JsonProperty("maxFanout")
        public double maxFanout = 4000.0;

        // how aggressively to widen probes / tables per round
        @JsonProperty("probeFactor")
        public double probeFactor = 1.5;
    }


    /* ======================== Exception type ======================== */

    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
