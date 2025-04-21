package com.fspann.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> {
    private final int capacity;
    private final Map<K, V> cache;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true); // LRU order
    }

    public V get(K key) {
        return cache.get(key);
    }

    public void put(K key, V value) {
        if (cache.size() >= capacity) {
            cache.remove(cache.entrySet().iterator().next().getKey());
        }
        cache.put(key, value);
    }

    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public void remove(K key) {
        cache.remove(key);
    }
}
