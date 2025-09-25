package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Application configuration loaded from a JSON or YAML file.
 * Backward compatible top-level knobs + structured nested sections.
 *
 * This version removes pre-sizing / biasing knobs from paper mode:
 * - targetMult (fanout)
 * - expandRadiusMax / expandRadiusHard
 * - fixed maxCandidates budgeting
 *
 * Fetch size is now an outcome of (m, lambda, divisions) and index distribution.
 * An optional safetyMaxCandidates exists purely as an OOM guard (disabled by default).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);

    // --- caps / guards ---
    private static final int  MAX_SHARDS            = 8192;
    private static final long MAX_OPS_THRESHOLD     = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD_MS  = 30L * 24 * 60 * 60 * 1000; // 30d
    private static final int  MAX_REENC_BATCH_SIZE  = 10_000;

    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();

    // --- existing top-level (back-compat) ---
    @JsonProperty("numShards")        private int  numShards       = 32;
    @JsonProperty("numTables")        private int  numTables       = 8;
    @JsonProperty("opsThreshold")     private long opsThreshold    = 500_000_000L;
    @JsonProperty("ageThresholdMs")   private long ageThresholdMs  = 7L * 24 * 60 * 60 * 1000; // 7d
    @JsonProperty("reEncBatchSize")   private int  reEncBatchSize  = 2_000;
    @JsonProperty("profilerEnabled")  private boolean profilerEnabled = true;
    @JsonProperty("reencryption")  private ReencryptionConfig reencryption = new ReencryptionConfig();
    @JsonProperty("reencryptionEnabled") private boolean reencryptionEnabled = true;

    // --- nested sections ---
    @JsonProperty("eval")    private EvalConfig   eval   = new EvalConfig();
    @JsonProperty("ratio")   private RatioConfig  ratio  = new RatioConfig();
    @JsonProperty("audit")   private AuditConfig  audit  = new AuditConfig();
    @JsonProperty("output")  private OutputConfig output = new OutputConfig();
    @JsonProperty("cloak")   private CloakConfig  cloak  = new CloakConfig();
    @JsonProperty("lsh")     private LshConfig    lsh    = new LshConfig();
    @JsonProperty("paper")   private PaperConfig  paper  = new PaperConfig();

    public SystemConfig() {} // for Jackson

    // ---------------- getters (top-level) ----------------
    public int getNumShards() { return numShards; }
    public int getNumTables() { return numTables; }
    public long getOpsThreshold() { return opsThreshold; }
    public long getAgeThresholdMs() { return ageThresholdMs; }
    public int getReEncBatchSize() { return reEncBatchSize; }
    public boolean isProfilerEnabled() { return profilerEnabled; }
    public boolean isReencryptionGloballyEnabled() {
        return (reencryption != null) ? reencryption.enabled : reencryptionEnabled;
    }
    public ReencryptionConfig getReencryption() { return reencryption; }

    // ---------------- getters (nested) ----------------
    public EvalConfig getEval() { return eval; }
    public RatioConfig getRatio() { return ratio; }
    public AuditConfig getAudit() { return audit; }
    public OutputConfig getOutput() { return output; }
    public CloakConfig getCloak() { return cloak; }
    public LshConfig getLsh() { return lsh; }
    public PaperConfig getPaper() { return paper; }

    // ---------------- validation ----------------
    public void validate() throws ConfigLoadException {
        // top-level
        if (numShards <= 0) throw new ConfigLoadException("numShards must be positive", null);
        if (numShards > MAX_SHARDS) {
            logger.warn("numShards {} exceeds maximum {}, capping", numShards, MAX_SHARDS);
            numShards = MAX_SHARDS;
        }
        if (opsThreshold <= 0) throw new ConfigLoadException("opsThreshold must be positive", null);
        if (opsThreshold > MAX_OPS_THRESHOLD) {
            logger.warn("opsThreshold {} exceeds maximum {}, capping", opsThreshold, MAX_OPS_THRESHOLD);
            opsThreshold = MAX_OPS_THRESHOLD;
        }
        if (ageThresholdMs <= 0) throw new ConfigLoadException("ageThresholdMs must be positive", null);
        if (ageThresholdMs > MAX_AGE_THRESHOLD_MS) {
            logger.warn("ageThresholdMs {} exceeds maximum {}, capping", ageThresholdMs, MAX_AGE_THRESHOLD_MS);
            ageThresholdMs = MAX_AGE_THRESHOLD_MS;
        }
        if (reEncBatchSize <= 0) throw new ConfigLoadException("reEncBatchSize must be positive", null);
        if (reEncBatchSize > MAX_REENC_BATCH_SIZE) {
            logger.warn("reEncBatchSize {} exceeds maximum {}, capping", reEncBatchSize, MAX_REENC_BATCH_SIZE);
            reEncBatchSize = MAX_REENC_BATCH_SIZE;
        }
        if (reencryption == null) reencryption = new ReencryptionConfig();

        // nested sections sanity
        if (eval == null)   eval   = new EvalConfig();
        if (ratio == null)  ratio  = new RatioConfig();
        if (audit == null)  audit  = new AuditConfig();
        if (output == null) output = new OutputConfig();
        if (cloak == null)  cloak  = new CloakConfig();
        if (lsh == null)    lsh    = new LshConfig();
        if (paper == null)  paper  = new PaperConfig();

        // bounds
        if (ratio.gtMismatchTolerance < 0.0 || ratio.gtMismatchTolerance > 1.0) {
            logger.warn("ratio.gtMismatchTolerance={} outside [0,1], clamping", ratio.gtMismatchTolerance);
            ratio.gtMismatchTolerance = Math.max(0.0, Math.min(1.0, ratio.gtMismatchTolerance));
        }
        if (eval.kVariants == null || eval.kVariants.length == 0) {
            eval.kVariants = new int[]{1,20,40,60,80,100};
        }
        if (cloak.noise < 0.0) {
            logger.warn("cloak.noise < 0, setting to 0");
            cloak.noise = 0.0;
        }
        if (output.resultsDir == null || output.resultsDir.isBlank()) {
            output.resultsDir = "results";
        }
        // lsh overrides are optional; no caps beyond positivity
        if (lsh.numTables < 0) lsh.numTables = 0;
        if (lsh.rowsPerBand < 0) lsh.rowsPerBand = 0;
        if (lsh.probeShards < 0) lsh.probeShards = 0;

        // paper-mode: enforce positivity and disable safety cap if invalid
        if (paper.m <= 0) throw new ConfigLoadException("paper.m must be positive", null);
        if (paper.lambda <= 0) throw new ConfigLoadException("paper.lambda must be positive", null);
        if (paper.divisions <= 0) throw new ConfigLoadException("paper.divisions must be positive", null);
        if (paper.safetyMaxCandidates < 0) {
            logger.warn("paper.safetyMaxCandidates < 0; disabling safety cap");
            paper.safetyMaxCandidates = 0;
        }
    }

    // ---------------- loading ----------------
    public static SystemConfig load(String filePath) throws ConfigLoadException {
        return load(filePath, false);
    }

    public static SystemConfig load(String filePath, boolean refresh) throws ConfigLoadException {
        Objects.requireNonNull(filePath, "Config file path cannot be null");
        Path path = Paths.get(filePath).toAbsolutePath().normalize();
        String cacheKey = path.toString();

        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", path);
            throw new ConfigLoadException("Config file is not readable: " + path, null);
        }

        if (!refresh) {
            SystemConfig cached = configCache.get(cacheKey);
            if (cached != null) {
                logger.debug("Returning cached configuration for: {}", cacheKey);
                return cached;
            }
        }

        ObjectMapper mapper = (cacheKey.toLowerCase().endsWith(".yaml") || cacheKey.toLowerCase().endsWith(".yml"))
                ? new ObjectMapper(new YAMLFactory())
                : new ObjectMapper();

        try {
            logger.info("Loading configuration from: {}", cacheKey);
            SystemConfig config = mapper.readValue(new File(cacheKey), SystemConfig.class);
            config.validate();
            configCache.put(cacheKey, config);
            logger.info("Successfully loaded and validated config from {}", cacheKey);
            return config;
        } catch (IOException e) {
            logger.error("Failed to load config from {}", cacheKey, e);
            throw new ConfigLoadException("Unable to load configuration: " + e.getMessage(), e);
        }
    }

    public static void clearCache() { configCache.clear(); }

    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) { super(message, cause); }
    }

    // ---------------- nested POJOs ----------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EvalConfig {
        @JsonProperty("computePrecision")         public boolean computePrecision = false;
        @JsonProperty("writeGlobalPrecisionCsv")  public boolean writeGlobalPrecisionCsv = false;
        @JsonProperty("kVariants")                public int[]   kVariants = new int[]{1,20,40,60,80,100};
        public EvalConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RatioConfig {
        @JsonProperty("source")                 public String source = "auto"; // auto|gt|base
        @JsonProperty("gtSample")               public int    gtSample = 20;
        @JsonProperty("gtMismatchTolerance")    public double gtMismatchTolerance = 0.02;
        public RatioConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuditConfig {
        @JsonProperty("enable")      public boolean enable = false;
        @JsonProperty("k")           public int     k = 100;
        @JsonProperty("sampleEvery") public int     sampleEvery = 200;
        @JsonProperty("worstKeep")   public int     worstKeep = 50;
        public AuditConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutputConfig {
        @JsonProperty("resultsDir")            public String  resultsDir = "results";
        @JsonProperty("exportArtifacts")       public boolean exportArtifacts = false;
        @JsonProperty("suppressLegacyMetrics") public boolean suppressLegacyMetrics = false;
        public OutputConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CloakConfig {
        @JsonProperty("noise") public double noise = 0.0; // 0 disables noise
        public CloakConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LshConfig {
        @JsonProperty("numTables")   public int numTables = 0;   // 0 means "no override"
        @JsonProperty("rowsPerBand") public int rowsPerBand = 0; // 0 means "no override"
        @JsonProperty("probeShards") public int probeShards = 0; // 0 means "no override"
        public LshConfig() {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReencryptionConfig {
        @JsonProperty("enabled") public boolean enabled = true; // default on; set false to disable
        public ReencryptionConfig() {}
    }

    /**
     * Paper-aligned ANN configuration:
     * - Fetch size is emergent from (m, lambda, divisions), NOT pre-sized.
     * - Optional safetyMaxCandidates is an OOM guard (disabled by default).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PaperConfig {
        @JsonProperty("enabled")   public boolean enabled = false;
        @JsonProperty("m")         public int     m = 12;
        @JsonProperty("lambda")    public int     lambda = 6;
        @JsonProperty("divisions") public int     divisions = 8;
        @JsonProperty("seed")      public long    seed = 42L;

        // Optional: OOM safety only. <=0 disables (default).
        @JsonProperty("safetyMaxCandidates") public int safetyMaxCandidates = 0;

        public PaperConfig() {}
    }
}