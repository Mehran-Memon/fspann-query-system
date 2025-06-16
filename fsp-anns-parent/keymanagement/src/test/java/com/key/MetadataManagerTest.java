package com.key;

import com.fspann.common.MetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class MetadataManagerTest {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(MetadataManagerTest.class);
    private MetadataManager metadataManager;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        metadataManager = new MetadataManager();
        System.setProperty("user.dir", tempDir.toString()); // Set working dir for relative path
    }

    @Test
    void testLoadSaveAndRetrieveMetadata() {
        logger.debug("Working directory: {}", System.getProperty("user.dir"));
        String filePath = Paths.get(System.getProperty("user.dir"), "metadata.ser").toString();

        try {
            metadataManager.putVectorMetadata("vec1", "0", "1");
            logger.debug("Before save to {}", filePath);
            metadataManager.save(filePath);
            logger.debug("File exists after save: {}", new File(filePath).exists());
            logger.debug("After save to {}", filePath);

            MetadataManager newManager = new MetadataManager();
            logger.debug("Before load from {}", filePath);
            newManager.load(filePath);
            logger.debug("After load from {}", filePath);

            Map<String, String> meta = newManager.getVectorMetadata("vec1");
            assertEquals("0", meta.get("shardId"));
            assertEquals("1", meta.get("version"));
        } catch (MetadataManager.MetadataException e) {
            logger.error("Metadata operation failed", e);
            fail("Test failed due to MetadataException: " + e.getMessage());
        }
    }

    @Test
    void testInvalidLoadThrowsException() {
        assertThrows(MetadataManager.MetadataException.class, () -> metadataManager.load("nonexistent.ser"));
    }

    @Test
    void testNullInputThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> metadataManager.putVectorMetadata(null, "0", "1"));
    }
}