package com.fspann.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

public class EncryptedPointBufferTest {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBufferTest.class);
    private RocksDBMetadataManager metadataManager;
    private EncryptedPointBuffer buffer;
    private Path tempDbPath;
    private Path tempPointsDir;

    @BeforeEach
    public void setup() throws Exception {
        tempDbPath = Files.createTempDirectory("rocksdb_test_");
        tempPointsDir = Files.createTempDirectory("points_test_");
        metadataManager = new RocksDBMetadataManager(tempDbPath.toString(), tempPointsDir.toString());
        buffer = new EncryptedPointBuffer(tempPointsDir.toString(), metadataManager, 2);
        logger.info("Initialized RocksDB at {} and points at {}", tempDbPath, tempPointsDir);
    }

    @AfterEach
    public void tearDown() {
        logger.info("Cleaning up RocksDB at {} and points at {}", tempDbPath, tempPointsDir);
        if (metadataManager != null) {
            metadataManager.close();
        }
        try {
            Files.walk(tempDbPath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to clean up directory {}", tempDbPath, e);
        }
        try {
            Files.walk(tempPointsDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to clean up directory {}", tempPointsDir, e);
        }
    }

    @Test
    void testBufferFlush(@TempDir Path tempDir) throws IOException {
        String dbPath = tempDir.resolve("rocksdb").toString();
        String pointsPath = tempDir.resolve("points").toString();
        RocksDBMetadataManager manager = new RocksDBMetadataManager(dbPath, pointsPath);
        EncryptedPointBuffer buffer = new EncryptedPointBuffer(pointsPath, manager, 2);
        List<Integer> buckets = Arrays.asList(1, 2, 3);
        EncryptedPoint p1 = new EncryptedPoint("vec1", 1, new byte[]{0, 1}, new byte[]{2, 3}, 1, 128, buckets);
        EncryptedPoint p2 = new EncryptedPoint("vec2", 1, new byte[]{4, 5}, new byte[]{6, 7}, 1, 128, buckets);
        buffer.add(p1);
        buffer.add(p2);
        buffer.flush(1);
        assertEquals(0, buffer.getBufferSize(), "Expected buffer to be empty after flush");
        manager.close();
    }
}