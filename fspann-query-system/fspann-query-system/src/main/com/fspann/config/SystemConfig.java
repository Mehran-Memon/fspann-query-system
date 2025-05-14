package com.fspann.config;

public final class SystemConfig {
    private SystemConfig() {}

    /* shard / rotation settings */
    public static final int NUM_SHARDS        = 32;
    public static final int OPS_THRESHOLD     = 5_000_000;
    public static final long AGE_THRESHOLD_MS = 7L * 24 * 60 * 60 * 1000; // 7 days
    public static final int REENC_BATCH_SIZE  = 2_000;

    /* profiler settings */
    public static boolean PROFILER_ENABLED = true;
}

