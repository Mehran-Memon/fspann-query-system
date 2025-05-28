
// File: src/test/java/com/fspann/common/LRUCacheTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LRUCacheTest {
    @Test
    void basicPutGetEviction() {
        LRUCache<String, Integer> cache = new LRUCache<>(2);
        cache.put("a", 1);
        cache.put("b", 2);
        assertEquals(1, cache.get("a"));
        cache.put("c", 3); // should evict "b" (least recently used)
        assertTrue(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
        assertTrue(cache.containsKey("c"));
    }

    @Test
    void removeKey() {
        LRUCache<String, String> cache = new LRUCache<>(2);
        cache.put("x", "X");
        assertTrue(cache.containsKey("x"));
        cache.remove("x");
        assertFalse(cache.containsKey("x"));
    }

}
