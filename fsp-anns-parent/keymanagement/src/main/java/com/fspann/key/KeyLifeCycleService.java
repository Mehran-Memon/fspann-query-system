package com.fspann.key;

/**
 * Manages forward‑secure key lifecycle: retrieval and rotation trigger.
 */
public interface KeyLifeCycleService {
    /**
     * Get the currently active key version.
     */
    KeyVersion getCurrentVersion();

    /**
     * Rotate keys when policy thresholds are met.
     */
    void rotateIfNeeded();
}
