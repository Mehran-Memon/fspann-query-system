package com.fspann.crypto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ReencryptionTracker: Tracks which vector IDs have been touched during queries.
 *
 * Used for selective re-encryption at the end of a query batch.
 * Thread-safe implementation with ConcurrentHashMap.
 */
public class ReencryptionTracker {

    private static final Logger log = LoggerFactory.getLogger(ReencryptionTracker.class);

    // Track all touched IDs across session
    private final Set<String> allTouched = ConcurrentHashMap.newKeySet();

    // Track touches per query for diagnostics
    private final Map<Integer, Set<String>> perQuery = new ConcurrentHashMap<>();

    private volatile int totalTouches = 0;

    public ReencryptionTracker() {
        log.debug("ReencryptionTracker initialized");
    }

    /**
     * Record a set of touched vector IDs from a query.
     * Accumulates into the global touched set.
     */
    public void record(Set<String> touchedIds) {
        if (touchedIds == null || touchedIds.isEmpty()) {
            return;
        }

        // Add all to global set
        allTouched.addAll(touchedIds);
        totalTouches += touchedIds.size();

        log.debug("Recorded {} touched IDs (total unique: {})",
                touchedIds.size(), allTouched.size());
    }

    /**
     * Record touches for a specific query (for diagnostics).
     */
    public void recordQuery(int queryIndex, Set<String> touchedIds) {
        if (touchedIds == null || touchedIds.isEmpty()) {
            return;
        }

        perQuery.put(queryIndex, new HashSet<>(touchedIds));
        record(touchedIds);
    }

    /**
     * Get all touched IDs accumulated so far.
     */
    public Set<String> getTouched() {
        return new HashSet<>(allTouched);
    }

    /**
     * Drain all touched IDs and clear the tracker.
     * This is typically called when committing re-encryption.
     *
     * @return a new Set containing all touched IDs (copy)
     */
    public Set<String> drainAll() {
        Set<String> result = new HashSet<>(allTouched);
        clear();
        return result;
    }

    /**
     * Get unique count of touched IDs.
     */
    public int uniqueCount() {
        return allTouched.size();
    }

    /**
     * Get total number of individual touches (may have duplicates).
     */
    public int totalTouches() {
        return totalTouches;
    }

    /**
     * Clear all recorded touches (typically at start of new session).
     */
    public void clear() {
        allTouched.clear();
        perQuery.clear();
        totalTouches = 0;
        log.debug("ReencryptionTracker cleared");
    }

    /**
     * Get touches for a specific query.
     */
    public Set<String> getQueryTouches(int queryIndex) {
        Set<String> touches = perQuery.get(queryIndex);
        return (touches != null) ? new HashSet<>(touches) : Collections.emptySet();
    }

    /**
     * Check if any IDs have been touched.
     */
    public boolean hasTouched() {
        return !allTouched.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(
                "ReencryptionTracker{unique=%d, total=%d, queries=%d}",
                allTouched.size(), totalTouches, perQuery.size()
        );
    }
}