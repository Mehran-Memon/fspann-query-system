package com.fspann.keymanagement;


/**
 * Policy settings governing when and how keys should be rotated.
 */
public class KeyRotationPolicy {
    private final int maxOperations;
    private final long maxIntervalMillis;
    private final long shardRotationTimeoutMillis;
    private final int maxKeyUsage;

    /**
     * @param maxOperations             number of operations before rotation
     * @param maxIntervalMillis         time interval in milliseconds before rotation
     * @param shardRotationTimeoutMillis timeout in milliseconds waiting for shard ops
     * @param maxKeyUsage               maximum usage count before rotation
     */
    public KeyRotationPolicy(int maxOperations,
                             long maxIntervalMillis,
                             long shardRotationTimeoutMillis,
                             int maxKeyUsage) {
        this.maxOperations = maxOperations;
        this.maxIntervalMillis = maxIntervalMillis;
        this.shardRotationTimeoutMillis = shardRotationTimeoutMillis;
        this.maxKeyUsage = maxKeyUsage;
    }

    /** @return max number of operations before triggering rotation */
    public int getMaxOperations() {
        return maxOperations;
    }

    /** @return max time interval (ms) before triggering rotation */
    public long getMaxIntervalMillis() {
        return maxIntervalMillis;
    }

    /** @return timeout (ms) to wait for shard operations on rotation */
    public long getShardRotationTimeout() {
        return shardRotationTimeoutMillis;
    }

    /** @return max usage count per key before rotation */
    public int getMaxKeyUsage() {
        return maxKeyUsage;
    }
}