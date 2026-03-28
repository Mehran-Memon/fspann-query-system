package com.fspann.key;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KeyUsageTracker.
 * Tests tracking, re-encryption, and safe deletion logic.
 */
public class KeyUsageTrackerTest {

    private KeyUsageTracker tracker;

    @BeforeEach
    public void setup() {
        tracker = new KeyUsageTracker();
    }

    @Test
    public void testTrackEncryption() {
        tracker.trackEncryption("vec1", 1);
        assertEquals(1, tracker.getVectorCount(1));
        assertFalse(tracker.isSafeToDelete(1));
    }

    @Test
    public void testTrackMultipleVectors() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec2", 1);
        tracker.trackEncryption("vec3", 1);

        assertEquals(3, tracker.getVectorCount(1));
        assertFalse(tracker.isSafeToDelete(1));
    }

    @Test
    public void testReencryption() {
        tracker.trackEncryption("vec1", 1);
        assertEquals(1, tracker.getVectorCount(1));

        tracker.trackReencryption("vec1", 1, 2);

        assertEquals(0, tracker.getVectorCount(1));
        assertTrue(tracker.isSafeToDelete(1));
        assertEquals(1, tracker.getVectorCount(2));
        assertFalse(tracker.isSafeToDelete(2));
    }

    @Test
    public void testPartialReencryption() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec2", 1);
        tracker.trackEncryption("vec3", 1);

        tracker.trackReencryption("vec1", 1, 2);
        tracker.trackReencryption("vec2", 1, 2);

        assertEquals(1, tracker.getVectorCount(1));
        assertFalse(tracker.isSafeToDelete(1));
        assertEquals(2, tracker.getVectorCount(2));
    }

    @Test
    public void testOverwriteTracking() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec1", 1);

        assertEquals(1, tracker.getVectorCount(1));
    }

    @Test
    public void testGetTrackedVersions() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec2", 2);
        tracker.trackEncryption("vec3", 3);

        Set<Integer> versions = tracker.getTrackedVersions();
        assertEquals(3, versions.size());
        assertTrue(versions.contains(1));
        assertTrue(versions.contains(2));
        assertTrue(versions.contains(3));
    }

    @Test
    public void testClear() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec2", 2);

        tracker.clear();

        assertEquals(0, tracker.getVectorCount(1));
        assertEquals(0, tracker.getVectorCount(2));
        assertTrue(tracker.getTrackedVersions().isEmpty());
    }

    @Test
    public void testSafeToDeleteWithNoVectors() {
        assertTrue(tracker.isSafeToDelete(99));

        tracker.trackEncryption("vec1", 1);
        tracker.trackReencryption("vec1", 1, 2);
        assertTrue(tracker.isSafeToDelete(1));
    }

    @Test
    public void testGetSummary() {
        tracker.trackEncryption("vec1", 1);
        tracker.trackEncryption("vec2", 1);
        tracker.trackEncryption("vec3", 2);

        String summary = tracker.getSummary();
        assertTrue(summary.contains("v1: 2 vectors"));
        assertTrue(summary.contains("v2: 1 vectors"));
    }
}