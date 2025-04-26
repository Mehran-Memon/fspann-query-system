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

    /**
     * Retrieves a value from the cache based on the key.
     * @param key The key for the cached value.
     * @return The value associated with the key, or null if the key is not found.
     */
    public V get(K key) {
        return cache.get(key); // Return the value if present, or null if absent
    }

    /**
     * Puts a value in the cache, evicting the least recently used item if the cache exceeds the capacity.
     * @param key The key for the value.
     * @param value The value to cache.
     */
    public void put(K key, V value) {
        if (cache.size() >= capacity) {
            // Evict the least recently used item
            cache.remove(cache.entrySet().iterator().next().getKey());
        }
        cache.put(key, value); // Add the new item to the cache
    }

    /**
     * Checks if the cache contains a value for the given key.
     * @param key The key to check.
     * @return True if the cache contains the key, otherwise false.
     */
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    /**
     * Removes a value from the cache by its key.
     * @param key The key to remove.
     */
    public void remove(K key) {
        cache.remove(key); // Remove the key-value pair from the cache
    }
}
