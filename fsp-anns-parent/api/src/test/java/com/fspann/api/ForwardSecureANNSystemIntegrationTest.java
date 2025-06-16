package com.fspann.api;

import com.fspann.common.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemIntegrationTest.class);

    @BeforeEach
    public void setUp() throws IOException {
        Files.deleteIfExists(Paths.get("keys.ser"));
        Files.deleteIfExists(Paths.get("rotation_*.meta"));
    }

    @Test
    public void simpleEndToEndNearestNeighbor(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");

        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                2
        );

        logger.info("Indexed vectors: {}", localSys.getIndexedVectorCount());
        assertTrue(localSys.getIndexedVectorCount() > 0, "No vectors indexed");

        logger.info("Executing query for vector: [0.05, 0.05]");
        // Ensure key and IV are properly initialized in the system
        List<QueryResult> res = localSys.queryWithCloak(new double[]{0.05, 0.05}, 1);

        logger.info("Query results count: {}", res.size());
        for (QueryResult r : res) {
            logger.info("ID: {}, Distance: {}", r.getId(), r.getDistance());
        }

        assertEquals(1, res.size());
        assertEquals(Math.hypot(0.05 - 0.1, 0.05 - 0.1), res.get(0).getDistance(), 0.05);
    }
}