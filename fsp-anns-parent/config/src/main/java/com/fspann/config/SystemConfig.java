package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
 * Centralizes shard/tables/rotation settings, evaluation & output toggles,
 * and optional "paper" engine parameters.
 *
 * Backwards-compatible: existing fields/getters remain and map to sensible defaults.
 */
public class SystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);

    // ---- global caps ----
    private static final int  MAX_SHARDS            = 8192;
    private static final long MAX_OPS_THRESHOLD     = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD_MS  = 30L * 24 * 60 * 60 * 1000; // 30 days
    private static final int  MAX_REENC_BATCH_SIZE  = 100_000;
    private static final int  MAX_TABLES            = 4096;
    private static final int  MAX_ROWS_PER_BAND     = 4096;
    private static final int  MAX_PROBE_SHARDS      = 65536;

    // cache per absolute normalized path
    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();

    /* ---------------------- legacy / top-level (kept) ---------------------- */
    @JsonProperty("numShards")       private int  numShards       = 32;
    @JsonProperty("numTables")       private int  numTables       = 8;        // LSH tables (ℓ). Also mirrored in lsh.numTables if provided.
    @JsonProperty("opsThreshold")    private long opsThreshold    = 500_000_000L;
    @JsonProperty("ageThresholdMs")  private long ageThresholdMs  = 7L * 24 * 60 * 60 * 1000; // 7 days
    @JsonProperty("reEncBatchSize")  private int  reEncBatchSize  = 2_000;
    @JsonProperty("profilerEnabled") private boolean profilerEnabled = true;

    /* ---------------------- new structured sections ---------------------- */

    /** LSH / probing knobs (optional). */
    @JsonProperty("lsh") private LshConfig lsh = new LshConfig();

    /** Evaluation toggles: precision, global recall writing, etc. */
    @JsonProperty("eval") private EvalConfig eval = new EvalConfig();

    /** Ratio calculation strategy and GT trust checks. */
    @JsonProperty("ratio") private RatioConfig ratio = new RatioConfig();

    /** Audit settings for sampling / worst-case capture. */
    @JsonProperty("audit") private AuditConfig audit = new AuditConfig();

    /** Output/exports: where to write files and which ones to produce. */
    @JsonProperty("output") private OutputConfig output = new OutputConfig();

    /** Cloak/noise controls for privacy-preserving query transforms. */
    @JsonProperty("cloak") private CloakConfig cloak = new CloakConfig();

    /** Optional paper engine params. */
    @JsonProperty("paper") private PaperConfig paper = new PaperConfig();

    public SystemConfig() {} // for Jackson

    /* ---------------------- getters (legacy kept) ---------------------- */
    public int getNumShards() { return numShards; }
    public int getNumTables() { return (lsh != null && lsh.numTables > 0) ? lsh.numTables : numTables; }
    public long getOpsThreshold() { return opsThreshold; }
    public long getAgeThresholdMs() { return ageThresholdMs; }
    public int getReEncBatchSize() { return reEncBatchSize; }
    public boolean isProfilerEnabled() { return profilerEnabled; }

    /* ---------------------- getters (new sections) ---------------------- */
    public LshConfig getLsh() { return lsh; }
    public EvalConfig getEval() { return eval; }
    public RatioConfig getRatio() { return ratio; }
    public AuditConfig getAudit() { return audit; }
    public OutputConfig getOutput() { return output; }
    public CloakConfig getCloak() { return cloak; }
    public PaperConfig getPaper() { return paper; }

    /* ---------------------- validation ---------------------- */
    public void validate() throws ConfigLoadException {
        // legacy / core
        if (numShards <= 0) throw new ConfigLoadException("numShards must be positive", null);
        if (numShards > MAX_SHARDS) {
            logger.warn("numShards {} exceeds maximum {}, capping", numShards, MAX_SHARDS);
            numShards = MAX_SHARDS;
        }

        int effectiveTables = getNumTables();
        if (effectiveTables <= 0) throw new ConfigLoadException("numTables must be positive", null);
        if (effectiveTables > MAX_TABLES) {
            logger.warn("numTables {} exceeds maximum {}, capping", effectiveTables, MAX_TABLES);
            // cap both top-level and nested mirror to keep them consistent
            numTables = Math.min(numTables, MAX_TABLES);
            if (lsh != null) lsh.numTables = Math.min(lsh.numTables, MAX_TABLES);
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

        // lsh section
        if (lsh == null) lsh = new LshConfig();
        if (lsh.rowsPerBand < -1 || lsh.rowsPerBand == 0) {
            throw new ConfigLoadException("lsh.rowsPerBand must be -1 (unset) or positive", null);
        }
        if (lsh.rowsPerBand > MAX_ROWS_PER_BAND) {
            logger.warn("lsh.rowsPerBand {} exceeds {}, capping", lsh.rowsPerBand, MAX_ROWS_PER_BAND);
            lsh.rowsPerBand = MAX_ROWS_PER_BAND;
        }
        if (lsh.probeShards < -1 || lsh.probeShards == 0) {
            throw new ConfigLoadException("lsh.probeShards must be -1 (unset) or positive", null);
        }
        if (lsh.probeShards > MAX_PROBE_SHARDS) {
            logger.warn("lsh.probeShards {} exceeds {}, capping", lsh.probeShards, MAX_PROBE_SHARDS);
            lsh.probeShards = MAX_PROBE_SHARDS;
        }
        if (lsh.candidateCap < 0) {
            throw new ConfigLoadException("lsh.candidateCap must be >= 0 (0 = unlimited)", null);
        }

        // eval section
        if (eval == null) eval = new EvalConfig();
        if (eval.kVariants == null || eval.kVariants.length == 0) {
            eval.kVariants = new int[]{1, 20, 40, 60, 80, 100};
        }

        // ratio section
        if (ratio == null) ratio = new RatioConfig();
        if (ratio.gtSample < 0) ratio.gtSample = 0;
        if (ratio.gtMismatchTolerance < 0.0 || ratio.gtMismatchTolerance > 1.0) {
            logger.warn("ratio.gtMismatchTolerance {} out of [0,1], clamping", ratio.gtMismatchTolerance);
            ratio.gtMismatchTolerance = Math.max(0.0, Math.min(1.0, ratio.gtMismatchTolerance));
        }
        if (!ratio.source.equalsIgnoreCase("auto") &&
                !ratio.source.equalsIgnoreCase("gt") &&
                !ratio.source.equalsIgnoreCase("base")) {
            throw new ConfigLoadException("ratio.source must be one of: auto | gt | base", null);
        }

        // audit section
        if (audit == null) audit = new AuditConfig();
        if (audit.k <= 0) audit.k = 100;
        if (audit.sampleEvery <= 0) audit.sampleEvery = 200;
        if (audit.worstKeep < 0) audit.worstKeep = 0;

        // output section
        if (output == null) output = new OutputConfig();
        if (output.resultsDir == null || output.resultsDir.isBlank()) {
            output.resultsDir = System.getProperty("results.dir", "results");
        }

        // cloak section
        if (cloak == null) cloak = new CloakConfig();
        if (cloak.noise < 0.0) cloak.noise = 0.0;

        // paper section
        if (paper == null) paper = new PaperConfig();
        if (paper.m <= 0) paper.m = 12;
        if (paper.lambda <= 0) paper.lambda = 6;
        if (paper.divisions <= 0) paper.divisions = 8;
        // seed may be any long
    }

    /* ---------------------- load & cache ---------------------- */

    /** Load configuration (JSON or YAML). Uses an internal cache. */
    public static SystemConfig load(String filePath) throws ConfigLoadException {
        return load(filePath, false);
    }

    /**
     * Load configuration with optional cache refresh.
     * @param filePath path to config file (json/yaml)
     * @param refresh  if true, bypass the cache and reload from disk
     */
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

    /** Clear the in-memory cache (useful for tests). */
    public static void clearCache() { configCache.clear(); }

    /* ---------------------- nested types ---------------------- */

    /** Knobs for LSH and probing. Optional values use -1 to mean "unset / use defaults". */
    public static class LshConfig {
        /** Mirror for numTables; if set (>0), overrides top-level numTables. */
        @JsonProperty("numTables") public int numTables = -1;
        /** m (rows per band / hash funcs per table). -1 = leave implementation default. */
        @JsonProperty("rowsPerBand") public int rowsPerBand = -1;
        /** shards to probe for each token. -1 = use index default / numShards. */
        @JsonProperty("probeShards") public int probeShards = -1;
        /** Optional cap on candidate count (0 = unlimited). */
        @JsonProperty("candidateCap") public int candidateCap = 0;
        /** Optional bucket width (if your LSH supports it). 0 = leave default. */
        @JsonProperty("bucketWidth") public double bucketWidth = 0.0;
    }

    /** Evaluation toggles. */
    public static class EvalConfig {
        /** Compute precision@K alongside ratio? */
        @JsonProperty("computePrecision") public boolean computePrecision = false;
        /** When true, write global_recall.csv at the end. */
        @JsonProperty("writeGlobalRecallCsv") public boolean writeGlobalRecallCsv = false;
        /** K variants list for per-query + global aggregation. */
        @JsonProperty("kVariants") public int[] kVariants = new int[]{1, 20, 40, 60, 80, 100};
    }

    /** Ratio settings & GT trust. */
    public static class RatioConfig {
        /** auto | gt | base */
        @JsonProperty("source") public String source = "auto";
        /** Number of random queries to sample for GT trust. */
        @JsonProperty("gtSample") public int gtSample = 20;
        /** Allowed mismatch rate in [0,1] for GT trust check. */
        @JsonProperty("gtMismatchTolerance") public double gtMismatchTolerance = 0.02;
    }

    /** Audit controls for sampling/worst cases. */
    public static class AuditConfig {
        @JsonProperty("enable") public boolean enable = false;
        @JsonProperty("k") public int k = 100;
        @JsonProperty("sampleEvery") public int sampleEvery = 200;
        @JsonProperty("worstKeep") public int worstKeep = 50;
    }

    /** Output & export toggles. */
    public static class OutputConfig {
        /** Root folder for results, CSVs, and artifacts. */
        @JsonProperty("resultsDir") public String resultsDir = System.getProperty("results.dir", "results");
        /** Write profiler CSVs (timelines, meters)? */
        @JsonProperty("writeProfilerCsvs") public boolean writeProfilerCsvs = true;
        /** Write per-query TopK evaluation CSV? */
        @JsonProperty("writeTopKEvalCsv") public boolean writeTopKEvalCsv = true;
        /** Export artifacts (queries.csv, summary, etc.)? */
        @JsonProperty("exportArtifacts") public boolean exportArtifacts = false;
        /** If true, suppress any extra/legacy metric files we don’t care about. */
        @JsonProperty("suppressLegacyMetrics") public boolean suppressLegacyMetrics = true;
    }

    /** Cloak / noise injection for query obfuscation. */
    public static class CloakConfig {
        /** Additive Gaussian scale; 0.0 disables noise. */
        @JsonProperty("noise") public double noise = 0.0;
    }

    /** Optional paper engine configuration. */
    public static class PaperConfig {
        @JsonProperty("enabled")   public boolean enabled   = false;
        @JsonProperty("m")         public int     m         = 12;
        @JsonProperty("lambda")    public int     lambda    = 6;
        @JsonProperty("divisions") public int     divisions = 8;
        @JsonProperty("seed")      public long    seed      = 42L;
    }

    /* ---------------------- helpers ---------------------- */
    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) { super(message, cause); }
    }

    @JsonIgnore
    @Override public String toString() {
        return "SystemConfig{" +
                "numShards=" + numShards +
                ", numTables=" + getNumTables() +
                ", opsThreshold=" + opsThreshold +
                ", ageThresholdMs=" + ageThresholdMs +
                ", reEncBatchSize=" + reEncBatchSize +
                ", profilerEnabled=" + profilerEnabled +
                ", lsh=" + lsh +
                ", eval=" + eval +
                ", ratio=" + ratio +
                ", audit=" + audit +
                ", output=" + output +
                ", cloak=" + cloak +
                ", paper=" + paper +
                '}';
    }
}