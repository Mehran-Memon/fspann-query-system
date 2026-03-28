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

        private final Set<String> allTouched = ConcurrentHashMap.newKeySet();
        private final Map<Integer, Set<String>> perQuery = new ConcurrentHashMap<>();
        private volatile int totalTouches = 0;

        public void record(Set<String> touchedIds) {
            if (touchedIds == null || touchedIds.isEmpty()) return;
            allTouched.addAll(touchedIds);
            totalTouches += touchedIds.size();
        }

        public void recordQuery(int queryIndex, Set<String> touchedIds) {
            if (touchedIds == null || touchedIds.isEmpty()) return;
            perQuery.put(queryIndex, new HashSet<>(touchedIds));
            record(touchedIds);
        }

        public Set<String> drainTouchedIds() {
            Set<String> out = new HashSet<>(allTouched);
            allTouched.clear();
            perQuery.clear();
            totalTouches = 0;
            return out;
        }

        public int uniqueCount() {
            return allTouched.size();
        }

        @Override
        public String toString() {
            return "ReencryptionTracker{unique=" + allTouched.size()
                    + ", total=" + totalTouches
                    + ", queries=" + perQuery.size() + "}";
        }
    }