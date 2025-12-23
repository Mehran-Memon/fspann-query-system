package com.fspann.key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks which vectors are encrypted with which key versions.
 * Enables safe key deletion by verifying no vectors remain bound to old keys.
 *
 * Thread-safe for concurrent encryption/re-encryption operations.
 */
public class KeyUsageTracker {
    private static final Logger logger = LoggerFactory.getLogger(KeyUsageTracker.class);

    // Map: keyVersion -> Set<vectorId>
    private final ConcurrentMap<Integer, Set<String>> keyToVectors = new ConcurrentHashMap<>();

    // Map: vectorId -> keyVersion (for fast lookups)
    private final ConcurrentMap<String, Integer> vectorToKey = new ConcurrentHashMap<>();

    /**
     * Record that a vector was encrypted with a specific key version.
     */
    public void trackEncryption(String vectorId, int keyVersion) {
        if (vectorId == null) return;

        // Update vector->key mapping
        Integer oldVersion = vectorToKey.put(vectorId, keyVersion);

        // Remove from old version's set if exists
        if (oldVersion != null && oldVersion != keyVersion) {
            Set<String> oldSet = keyToVectors.get(oldVersion);
            if (oldSet != null) {
                oldSet.remove(vectorId);
            }
        }

        // Add to new version's set
        keyToVectors.computeIfAbsent(keyVersion, k -> ConcurrentHashMap.newKeySet())
                .add(vectorId);

        logger.trace("Tracked: vector {} -> key v{}", vectorId, keyVersion);
    }

    /**
     * Record that a vector was re-encrypted from oldVersion to newVersion.
     */
    public void trackReencryption(String vectorId, int oldVersion, int newVersion) {
        if (vectorId == null) return;

        // Remove from old version
        Set<String> oldSet = keyToVectors.get(oldVersion);
        if (oldSet != null) {
            oldSet.remove(vectorId);
        }

        // Update to new version
        vectorToKey.put(vectorId, newVersion);
        keyToVectors.computeIfAbsent(newVersion, k -> ConcurrentHashMap.newKeySet())
                .add(vectorId);

        logger.trace("Re-encrypted: vector {} from v{} -> v{}", vectorId, oldVersion, newVersion);
    }

    /**
     * Check if a key version is safe to delete (no vectors use it).
     */
    public boolean isSafeToDelete(int keyVersion) {
        Set<String> vectors = keyToVectors.get(keyVersion);
        boolean safe = (vectors == null || vectors.isEmpty());

        if (!safe) {
            logger.warn("Key v{} NOT safe to delete: {} vectors still bound",
                    keyVersion, vectors.size());
        }

        return safe;
    }

    /**
     * Get count of vectors bound to a key version.
     */
    public int getVectorCount(int keyVersion) {
        Set<String> vectors = keyToVectors.get(keyVersion);
        return (vectors != null) ? vectors.size() : 0;
    }

    /**
     * Get all key versions currently tracked.
     */
    public Set<Integer> getTrackedVersions() {
        return new HashSet<>(keyToVectors.keySet());
    }

    /**
     * Clear all tracking data (for testing or reset).
     */
    public void clear() {
        keyToVectors.clear();
        vectorToKey.clear();
        logger.debug("KeyUsageTracker cleared");
    }

    /**
     * Get diagnostic summary.
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder("KeyUsageTracker{\n");
        List<Integer> versions = new ArrayList<>(keyToVectors.keySet());
        Collections.sort(versions);

        for (Integer v : versions) {
            int count = getVectorCount(v);
            sb.append(String.format("  v%d: %d vectors\n", v, count));
        }
        sb.append("}");
        return sb.toString();
    }
}