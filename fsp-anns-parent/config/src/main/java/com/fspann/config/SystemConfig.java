package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Canonical, paper-aligned configuration for FSP-ANN (MSANNP).
 *
 * RULES:
 * - Every field here is used by the system.
 * - No speculative or half-wired knobs.
 * - JSON structure is flat and deterministic.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SystemConfig {

    // ---------------- limits ----------------

    private static final int  MAX_SHARDS        = 8192;
    private static final long MAX_OPS_THRESHOLD = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD = 365L * 24L * 60L * 60L * 1000L;

    // ---------------- loader ----------------

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ConcurrentMap<String, SystemConfig> CACHE =
            new ConcurrentHashMap<>();

    // ================= top-level =================

    @JsonProperty("profiles")
    private ProfileConfig[] profiles;

    @JsonProperty("numShards")
    private int numShards = 32;

    @JsonProperty("opsThreshold")
    private long opsThreshold = 500_000_000L;

    @JsonProperty("ageThresholdMs")
    private long ageThresholdMs = 24L * 60L * 60L * 1000L;

    @JsonProperty("reencryptionEnabled")
    private boolean reencryptionEnabled = true;

    @JsonProperty("forwardSecurityEnabled")
    private boolean forwardSecurityEnabled = true;

    @JsonProperty("profilerEnabled")
    private boolean profilerEnabled = true;

    @JsonProperty("paper")
    private PaperConfig paper = new PaperConfig();

    @JsonProperty("runtime")
    private RuntimeConfig runtime = new RuntimeConfig();

    @JsonProperty("stabilization")
    private StabilizationConfig stabilization = new StabilizationConfig();

    @JsonProperty("eval")
    private EvalConfig eval = new EvalConfig();

    @JsonProperty("ratio")
    private RatioConfig ratio = new RatioConfig();

    @JsonProperty("reencryption")
    private ReencryptionConfig reencryption = new ReencryptionConfig();

    private KAdaptiveConfig kAdaptive;
    private OutputConfig output;
    private CloakConfig cloak;



    // ================= loading =================

    public static SystemConfig load(String path, boolean refresh)
            throws ConfigLoadException {

        Objects.requireNonNull(path, "config path");

        String key;
        try {
            Path p = Paths.get(path).toAbsolutePath().normalize();
            try { p = p.toRealPath(); } catch (IOException ignore) {}
            key = p.toString();
        } catch (Exception e) {
            throw new ConfigLoadException("Invalid config path", e);
        }

        if (!refresh) {
            SystemConfig cached = CACHE.get(key);
            if (cached != null) return cached;
        }

        SystemConfig cfg;
        try {
            Path p = Paths.get(key);
            if (!Files.isReadable(p)) {
                throw new IOException("Config not readable: " + key);
            }
            cfg = MAPPER.readValue(p.toFile(), SystemConfig.class);
        } catch (IOException e) {
            throw new ConfigLoadException("Failed to load config", e);
        }

        int maxK = Arrays.stream(cfg.getEval().kVariants).max().orElse(1);
        if (cfg.getRuntime().getMaxGlobalCandidates() < maxK) {
            throw new IllegalStateException(
                    "Invalid config: maxGlobalCandidates < maxK (" +
                            cfg.getRuntime().getMaxGlobalCandidates() + " < " + maxK + ")"
            );
        }

        cfg.numShards      = clamp(cfg.numShards, 1, MAX_SHARDS);
        cfg.opsThreshold   = clamp(cfg.opsThreshold, 1L, MAX_OPS_THRESHOLD);
        cfg.ageThresholdMs = clamp(cfg.ageThresholdMs, 0L, MAX_AGE_THRESHOLD);

        CACHE.put(key, cfg);
        return cfg;
    }

    // ================= getters =================

    public ProfileConfig[] getProfiles() { return profiles; }

    public int getNumShards() {
        return clamp(numShards, 1, MAX_SHARDS);
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

    public boolean isProfilerEnabled() {
        return profilerEnabled;
    }

    public boolean isReencryptionGloballyEnabled() {
        return reencryptionEnabled && reencryption.isEnabled();
    }

    public PaperConfig getPaper() { return paper; }
    public RuntimeConfig getRuntime() { return runtime; }
    public StabilizationConfig getStabilization() { return stabilization; }
    public EvalConfig getEval() { return eval; }
    public RatioConfig getRatio() { return ratio; }
    public ReencryptionConfig getReencryption() { return reencryption; }
    public KAdaptiveConfig getKAdaptive() {
        return kAdaptive;
    }
    public OutputConfig getOutput() {
        return output;
    }
    public CloakConfig getCloak() {
        return cloak;
    }
    // ================= nested configs =================

    public static final class PaperConfig {

        @JsonProperty("enabled")
        private boolean enabled = true;

        @JsonProperty("m")
        private int m = 24;

        @JsonProperty("lambda")
        private int lambda = 2;

        @JsonProperty("divisions")
        private int divisions = 3;

        @JsonProperty("tables")
        private int tables = 6;

        @JsonProperty("seed")
        private long seed = 13L;

        public boolean isEnabled() { return enabled; }
        public int getM() { return Math.max(1, m); }
        public int getLambda() { return Math.max(1, lambda); }
        public int getDivisions() { return Math.max(1, divisions); }
        public int getTables() { return Math.max(1, tables); }
        public long getSeed() { return seed; }
    }

    public static final class RuntimeConfig {

        @JsonProperty("maxCandidateFactor")
        private int maxCandidateFactor = 600;

        @JsonProperty("maxRefinementFactor")
        private int maxRefinementFactor = 200;

        @JsonProperty("maxRelaxationDepth")
        private int maxRelaxationDepth = Integer.MAX_VALUE;

        @JsonProperty("earlyStopCandidates")
        private int earlyStopCandidates = Integer.MAX_VALUE;

        @JsonProperty("refinementLimit")
        private int refinementLimit = 20_000;

        @JsonProperty("maxGlobalCandidates")
        private int maxGlobalCandidates = 20000;

        @JsonProperty("probeOverride")
        private int probeOverride = -1;

        public int getProbeOverride() {
            return probeOverride;
        }

        public int getMaxCandidateFactor() {
            return Math.max(1, maxCandidateFactor);
        }

        public int getMaxRefinementFactor() {
            return Math.max(1, maxRefinementFactor);
        }

        public int getMaxRelaxationDepth() {
            return Math.max(0, maxRelaxationDepth);
        }

        public int getEarlyStopCandidates() {
            return Math.max(1, earlyStopCandidates);
        }

        public int getRefinementLimit() {
            return Math.max(1, refinementLimit);
        }

        public int getMaxGlobalCandidates() {
            return Math.max(1, maxGlobalCandidates);
        }
    }

    public static final class StabilizationConfig {

        @JsonProperty("enabled")
        private boolean enabled = true;

        @JsonProperty("alpha")
        private double alpha = 0.06;

        @JsonProperty("minCandidatesRatio")
        private double minCandidatesRatio = 1.5;

        public boolean isEnabled() { return enabled; }

        public double getAlpha() {
            return clamp(alpha, 0.01, 1.0);
        }

        public double getMinCandidatesRatio() {
            return clamp(minCandidatesRatio, 1.0, 2.0);
        }
    }

    public static final class EvalConfig {

        @JsonProperty("computePrecision")
        public boolean computePrecision = true;

        @JsonProperty("writeGlobalPrecisionCsv")
        public boolean writeGlobalPrecisionCsv = true;

        @JsonProperty("kVariants")
        public int[] kVariants = {1, 5, 10, 20, 50, 100};

        public int getMaxK() {
            int max = 1;
            if (kVariants != null) {
                for (int k : kVariants) {
                    if (k > max) max = k;
                }
            }
            return max;
        }
    }

    public static final class RatioConfig {

        @JsonProperty("source")
        public String source = "gt";

        @JsonProperty("gtPath")
        public String gtPath;

        @JsonProperty("gtSample")
        public int gtSample = 100;

        @JsonProperty("gtMismatchTolerance")
        public double gtMismatchTolerance = 0.10;
    }

    public static final class ReencryptionConfig {

        @JsonProperty("enabled")
        private boolean enabled = true;

        @JsonProperty("batchSize")
        private int batchSize = 1024;

        @JsonProperty("maxMsPerBatch")
        private long maxMsPerBatch = 50L;

        public boolean isEnabled() { return enabled; }
        public int getBatchSize() { return Math.max(1, batchSize); }
        public long getMaxMsPerBatch() { return Math.max(0L, maxMsPerBatch); }
    }

    public static class KAdaptiveConfig {
        public boolean enabled = false;
        public double probeFactor = 2.0;
        public int maxFanout = 64;
    }

    public static class OutputConfig {
        public String resultsDir;
        public boolean exportArtifacts = true;
    }

    public static class CloakConfig {
        public double noise = 0.0;
    }

    // ================= profiles =================

    public static final class ProfileConfig {

        @JsonProperty("name")
        public String name;

        @JsonProperty("overrides")
        public OverrideConfig overrides;
    }

    public static final class OverrideConfig {

        @JsonProperty("paper")
        public PaperConfig paper;

        @JsonProperty("runtime")
        public RuntimeConfig runtime;

        @JsonProperty("stabilization")
        public StabilizationConfig stabilization;
    }

    // ================= helpers =================

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static long clamp(long v, long lo, long hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static double clamp(double v, double lo, double hi) {
        if (Double.isNaN(v)) return lo;
        if (v < lo) return lo;
        if (v > hi) return hi;
        return v;
    }

    public static final class ConfigLoadException extends Exception {
        public ConfigLoadException(String msg, Throwable t) {
            super(msg, t);
        }
    }
}
