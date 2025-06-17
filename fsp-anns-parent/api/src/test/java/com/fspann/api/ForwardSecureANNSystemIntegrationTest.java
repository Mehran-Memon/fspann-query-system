package com.fspann.api;

import com.fspann.common.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemIntegrationTest.class);

    @BeforeEach
    public void setUp() throws IOException {
        // Delete keys.ser
        Files.deleteIfExists(Paths.get("keys.ser"));

        // Delete rotation_*.meta files using DirectoryStream
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                Paths.get("."), "rotation_*.meta")) {
            for (Path path : stream) {
                Files.deleteIfExists(path);
                logger.debug("Deleted rotation meta file: {}", path);
            }
        } catch (IOException e) {
            logger.warn("Failed to delete rotation meta files", e);
        }
    }

    @Test
    public void simpleEndToEndNearestNeighbor(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");

        List<Integer> dimensions = Arrays.asList(2);
        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions
        );

        logger.info("Indexed vectors for dim=2: {}", localSys.getIndexedVectorCount(2));
        assertTrue(localSys.getIndexedVectorCount(2) >= 3, "Should have at least 3 vectors indexed");

        logger.info("Executing query for vector: [0.05, 0.05]");
        List<QueryResult> res = localSys.queryWithCloak(new double[]{0.05, 0.05}, 1, 2);

        logger.info("Query results count: {}", res.size());
        for (QueryResult r : res) {
            logger.info("ID: {}, Distance: {}", r.getId(), r.getDistance());
        }

        assertEquals(1, res.size(), "Should return exactly 1 result");
        assertEquals(Math.hypot(0.05 - 0.1, 0.05 - 0.1), res.get(0).getDistance(), 0.05, "Distance should be close to expected");
    }

    @Test
    public void testMultipleDimensions(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n0.0,0.0,0.0\n0.1,0.1,0.1\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");

        List<Integer> dimensions = Arrays.asList(2, 3);
        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions
        );

        assertTrue(localSys.getIndexedVectorCount(2) >= 3, "Should have at least 3 vectors for 2D");
        assertTrue(localSys.getIndexedVectorCount(3) >= 2, "Should have at least 2 vectors for 3D");

        List<QueryResult> res2D = localSys.queryWithCloak(new double[]{0.05, 0.05}, 1, 2);
        assertEquals(1, res2D.size(), "Should return 1 result for 2D query");
        assertEquals(Math.hypot(0.05 - 0.1, 0.05 - 0.1), res2D.get(0).getDistance(), 0.05, "2D distance should be close to expected");

        List<QueryResult> res3D = localSys.queryWithCloak(new double[]{0.05, 0.05, 0.05}, 1, 3);
        assertEquals(1, res3D.size(), "Should return 1 result for 3D query");
        assertEquals(Math.sqrt(3 * Math.pow(0.05 - 0.1, 2)), res3D.get(0).getDistance(), 0.05, "3D distance should be close to expected");
    }

    @Test
    public void testVisualization(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");

        List<Integer> dimensions = Arrays.asList(2);
        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions
        );

        localSys.visualizeEncryptionKeys();
        double[] queryVector = new double[]{0.05, 0.05};
        localSys.runEndToEnd(dataFile.toString(), queryVector, 1, 2);
    }
}