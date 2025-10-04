package com.fspann.crypto;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ReencryptionTracker {
    private final AtomicReference<Set<String>> touched =
            new AtomicReference<>(ConcurrentHashMap.newKeySet());

    public void touch(String id) {
        if (id != null) touched.get().add(id);
    }

    public int uniqueCount() { return touched.get().size(); }

    /** Atomically swaps the touched set with a fresh one and returns the previous contents. */
    public Set<String> drainAll() {
        return touched.getAndSet(ConcurrentHashMap.newKeySet());
    }
}
