package com.fspann.key;

/**
 * Settings governing when key rotation should occur.
 */
public class KeyRotationPolicy {
    private final int maxOperations;
    private final long maxIntervalMillis;

    public KeyRotationPolicy(int maxOperations, long maxIntervalMillis) {
        this.maxOperations = maxOperations;
        this.maxIntervalMillis = maxIntervalMillis;
    }

    public int getMaxOperations() { return maxOperations; }
    public long getMaxIntervalMillis() { return maxIntervalMillis; }
}
