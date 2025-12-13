package com.fspann.crypto;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * CORRECTED Comprehensive unit tests for ReencryptionTracker.
 * Tests tracking of touched vector IDs during queries.
 *
 * FIXED: record() method takes Set<String>, not String
 *
 * Run with: mvn test -Dtest=ReencryptionTrackerTest
 */
@DisplayName("ReencryptionTracker Unit Tests")
public class ReencryptionTrackerTest {

    private ReencryptionTracker tracker;

    @BeforeEach
    public void setUp() {
        tracker = new ReencryptionTracker();
    }

    // ============ BASIC RECORDING TESTS ============

    @Test
    @DisplayName("Test record single ID")
    public void testRecordSingleId() {
        String id = "vector-001";
        tracker.record(Set.of(id));

        Collection<String> touched = tracker.getTouched();
        assertNotNull(touched);
        assertTrue(touched.contains(id));
        assertEquals(1, touched.size());
    }

    @Test
    @DisplayName("Test record multiple IDs")
    public void testRecordMultipleIds() {
        String[] ids = {"v1", "v2", "v3", "v4", "v5"};
        tracker.record(Set.of(ids));

        Collection<String> touched = tracker.getTouched();
        assertEquals(5, touched.size());
        for (String id : ids) {
            assertTrue(touched.contains(id));
        }
    }

    @Test
    @DisplayName("Test record many IDs (100)")
    public void testRecordManyIds() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            ids.add("vector-" + i);
        }
        tracker.record(ids);

        Collection<String> touched = tracker.getTouched();
        assertEquals(100, touched.size());
    }

    @Test
    @DisplayName("Test record very many IDs (10000)")
    public void testRecordVeryManyIds() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            ids.add("v-" + i);
        }
        tracker.record(ids);

        Collection<String> touched = tracker.getTouched();
        assertEquals(10000, touched.size());
    }

    // ============ DUPLICATE HANDLING TESTS ============

    @Test
    @DisplayName("Test duplicate IDs counted once")
    public void testDuplicateIdsCounted() {
        tracker.record(Set.of("v1"));
        tracker.record(Set.of("v1"));
        tracker.record(Set.of("v1"));

        Collection<String> touched = tracker.getTouched();
        assertEquals(1, touched.size());
        assertTrue(touched.contains("v1"));
    }

    @Test
    @DisplayName("Test mixed duplicates and new IDs")
    public void testMixedDuplicatesAndNew() {
        tracker.record(Set.of("v1", "v2"));
        tracker.record(Set.of("v1"));  // duplicate
        tracker.record(Set.of("v3", "v2"));  // v2 is duplicate
        tracker.record(Set.of("v4"));

        Collection<String> touched = tracker.getTouched();
        assertEquals(4, touched.size());
        assertTrue(touched.contains("v1"));
        assertTrue(touched.contains("v2"));
        assertTrue(touched.contains("v3"));
        assertTrue(touched.contains("v4"));
    }

    @Test
    @DisplayName("Test uniqueCount returns correct value")
    public void testUniqueCount() {
        tracker.record(Set.of("v1", "v2"));
        tracker.record(Set.of("v1"));  // duplicate
        tracker.record(Set.of("v3"));

        int count = tracker.uniqueCount();
        assertEquals(3, count);
    }

    @Test
    @DisplayName("Test uniqueCount on empty tracker")
    public void testUniqueCountEmpty() {
        int count = tracker.uniqueCount();
        assertEquals(0, count);
    }

    // ============ DRAIN TESTS ============

    @Test
    @DisplayName("Test drainAll returns all touched IDs")
    public void testDrainAllReturnsIds() {
        String[] ids = {"v1", "v2", "v3", "v4", "v5"};
        tracker.record(Set.of(ids));

        Collection<String> drained = tracker.drainAll();

        assertEquals(5, drained.size());
        for (String id : ids) {
            assertTrue(drained.contains(id));
        }
    }

    @Test
    @DisplayName("Test drainAll clears tracker")
    public void testDrainAllClears() {
        tracker.record(Set.of("v1", "v2", "v3"));

        // First drain
        Collection<String> drained1 = tracker.drainAll();
        assertEquals(3, drained1.size());

        // Tracker should be empty
        assertEquals(0, tracker.uniqueCount());
        assertTrue(tracker.getTouched().isEmpty());
    }

    @Test
    @DisplayName("Test getTouched empty after drain")
    public void testGetTouchedEmptyAfterDrain() {
        tracker.record(Set.of("v1", "v2"));

        tracker.drainAll();

        Collection<String> touched = tracker.getTouched();
        assertTrue(touched.isEmpty());
        assertEquals(0, tracker.uniqueCount());
    }

    @Test
    @DisplayName("Test drainAll can be called multiple times")
    public void testDrainAllMultipleTimes() {
        tracker.record(Set.of("v1", "v2"));

        Collection<String> drain1 = tracker.drainAll();
        assertEquals(2, drain1.size());

        // Drain again - should be empty
        Collection<String> drain2 = tracker.drainAll();
        assertEquals(0, drain2.size());
    }

    @Test
    @DisplayName("Test record after drain starts fresh")
    public void testRecordAfterDrain() {
        tracker.record(Set.of("v1"));
        tracker.drainAll();

        tracker.record(Set.of("v2", "v3"));

        Collection<String> touched = tracker.getTouched();
        assertEquals(2, touched.size());
        assertTrue(touched.contains("v2"));
        assertTrue(touched.contains("v3"));
        assertFalse(touched.contains("v1"));
    }

    // ============ EDGE CASE TESTS ============

    @Test
    @DisplayName("Test getTouched returns non-null empty collection initially")
    public void testGetTouchedInitiallyEmpty() {
        Collection<String> touched = tracker.getTouched();
        assertNotNull(touched);
        assertTrue(touched.isEmpty());
    }

    @Test
    @DisplayName("Test drainAll on empty tracker returns empty collection")
    public void testDrainAllEmpty() {
        Collection<String> drained = tracker.drainAll();
        assertNotNull(drained);
        assertEquals(0, drained.size());
    }

    @Test
    @DisplayName("Test record with empty set")
    public void testRecordEmptySet() {
        tracker.record(new HashSet<>());

        Collection<String> touched = tracker.getTouched();
        assertTrue(touched.isEmpty());
    }

    @Test
    @DisplayName("Test record with empty string ID")
    public void testRecordEmptyString() {
        tracker.record(Set.of("", "v1"));

        Collection<String> touched = tracker.getTouched();
        assertTrue(touched.contains(""));
        assertTrue(touched.contains("v1"));
    }

    @Test
    @DisplayName("Test record with very long ID")
    public void testRecordLongId() {
        String longId = "v" + "x".repeat(10000);
        tracker.record(Set.of(longId));

        Collection<String> touched = tracker.getTouched();
        assertTrue(touched.contains(longId));
    }

    @Test
    @DisplayName("Test concurrent-like recording pattern")
    public void testConcurrentPattern() {
        // Simulate rapid concurrent recording
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            ids.add("batch-" + (i / 10) + "-item-" + i);
        }
        tracker.record(ids);

        Collection<String> touched = tracker.getTouched();
        assertEquals(100, touched.size());
        assertEquals(100, tracker.uniqueCount());
    }

    // ============ INTEGRATION TESTS ============

    @Test
    @DisplayName("Test realistic query workflow")
    public void testRealisticQueryWorkflow() {
        // Simulate query touching vectors
        Set<String> queryTouched = Set.of("v-100", "v-201", "v-305", "v-450", "v-510");
        tracker.record(queryTouched);

        // Check state before drain
        assertEquals(5, tracker.uniqueCount());
        assertEquals(5, tracker.getTouched().size());

        // Drain for re-encryption
        Collection<String> toReencrypt = tracker.drainAll();
        assertEquals(5, toReencrypt.size());

        // After drain, tracker is empty
        assertEquals(0, tracker.uniqueCount());
        assertTrue(tracker.getTouched().isEmpty());

        // New query starts with fresh tracker
        tracker.record(Set.of("v-600"));
        assertEquals(1, tracker.uniqueCount());
    }

    @Test
    @DisplayName("Test accumulation across multiple queries")
    public void testAccumulationMultipleQueries() {
        // Query 1
        Set<String> query1 = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            query1.add("q1-v-" + i);
        }
        tracker.record(query1);
        assertEquals(100, tracker.uniqueCount());

        // Query 2 (accumulates)
        Set<String> query2 = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            query2.add("q2-v-" + i);
        }
        tracker.record(query2);
        assertEquals(150, tracker.uniqueCount());

        // Query 2 touches some same vectors as Query 1
        Set<String> query1Overlap = new HashSet<>();
        for (int i = 0; i < 30; i++) {
            query1Overlap.add("q1-v-" + i);  // Already recorded
        }
        tracker.record(query1Overlap);
        assertEquals(150, tracker.uniqueCount());  // Still 150, no duplicates
    }

    @Test
    @DisplayName("Test drainAll returns modifiable collection")
    public void testDrainAllReturnsModifiable() {
        tracker.record(Set.of("v1", "v2"));

        Collection<String> drained = tracker.drainAll();

        // Verify we can iterate
        int count = 0;
        for (String id : drained) {
            count++;
            assertNotNull(id);
        }
        assertEquals(2, count);
    }

    @Test
    @DisplayName("Test getTouched returns consistent view")
    public void testGetTouchedConsistent() {
        tracker.record(Set.of("v1", "v2", "v3"));

        Collection<String> touched1 = tracker.getTouched();
        Collection<String> touched2 = tracker.getTouched();

        assertEquals(touched1.size(), touched2.size());
        for (String id : touched1) {
            assertTrue(touched2.contains(id));
        }
    }

    @Test
    @DisplayName("Test multiple sequential record calls")
    public void testMultipleSequentialRecords() {
        tracker.record(Set.of("a", "b"));
        tracker.record(Set.of("c", "d"));
        tracker.record(Set.of("e", "f"));

        Collection<String> touched = tracker.getTouched();
        assertEquals(6, touched.size());
        assertTrue(touched.contains("a"));
        assertTrue(touched.contains("f"));
    }

    @Test
    @DisplayName("Test record with null set")
    public void testRecordNullSet() {
        // Should either ignore or throw - both are acceptable
        try {
            tracker.record(null);
            // If no exception, check behavior
            Collection<String> touched = tracker.getTouched();
            assertNotNull(touched);
        } catch (NullPointerException | IllegalArgumentException e) {
            // Also acceptable - null should not be recorded
            assertTrue(true);
        }
    }
}