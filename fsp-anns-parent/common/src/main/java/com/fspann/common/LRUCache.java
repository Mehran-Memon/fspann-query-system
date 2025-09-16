package com.fspann.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple LRU (Least Recently Used) cache implementation.
 */
public class LRUCache<K, V> {
    private final int capacity;
    private final Map<K, V> cache;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > LRUCache.this.capacity;
            }
        };
    }

    public synchronized V get(K key) {
        return cache.get(key);
    }

    public synchronized void put(K key, V value) {
        cache.put(key, value);
    }

    public synchronized boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public synchronized void remove(K key) {
        cache.remove(key);
    }
}
