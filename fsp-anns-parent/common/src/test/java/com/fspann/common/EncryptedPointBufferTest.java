package com.fspann.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EncryptedPointBufferTest {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBufferTest.class);

    private RocksDBMetadataManager metadataManager;
    private EncryptedPointBuffer buffer;

    @TempDir
    Path tempRoot; // JUnit will clean this up

    private Path dbPath;
    private Path pointsPath;

    @BeforeEach
    public void setup() throws Exception {
        dbPath = tempRoot.resolve("rocksdb");
        pointsPath = tempRoot.resolve("points");

        // Use factory (singleton), not the private constructor
        metadataManager = RocksDBMetadataManager.create(dbPath.toString(), pointsPath.toString());
        buffer = new EncryptedPointBuffer(pointsPath.toString(), metadataManager, 2);

        logger.info("Initialized RocksDB at {} and points at {}", dbPath, pointsPath);
    }

    @AfterEach
    public void tearDown() {
        logger.info("Cleaning up RocksDB at {} and points at {}", dbPath, pointsPath);
        if (metadataManager != null) {
            try {
                metadataManager.close();
                // tiny wait helps Windows/JNI release LOCK/LOG handles
                try { Thread.sleep(50); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            } catch (Exception e) {
                logger.error("Error closing metadata manager", e);
            }
        }

        // Not strictly needed with @TempDir, but if you want to be explicit:
        try {
            if (Files.exists(tempRoot)) {
                Files.walk(tempRoot)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); }
                            catch (IOException e) { logger.error("Failed to delete {}", p, e); }
                        });
            }
        } catch (IOException e) {
            logger.error("Failed to clean up {}", tempRoot, e);
        }
    }

    @Test
    void testBufferFlush() {
        List<Integer> buckets = Arrays.asList(1, 2, 3);
        EncryptedPoint p1 = new EncryptedPoint("vec1", 1, new byte[]{0, 1}, new byte[]{2, 3}, 1, 128, buckets);
        EncryptedPoint p2 = new EncryptedPoint("vec2", 1, new byte[]{4, 5}, new byte[]{6, 7}, 1, 128, buckets);

        buffer.add(p1);
        buffer.add(p2);

        buffer.flush(1);

        assertEquals(0, buffer.getBufferSize(), "Expected buffer to be empty after flush");
    }
}
