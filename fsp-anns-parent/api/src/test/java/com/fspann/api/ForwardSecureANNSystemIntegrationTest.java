package com.fspann.api;

import com.fspann.common.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemIntegrationTest.class);

    @BeforeEach
    public void setUp() throws Exception {
        // No-op: ForwardSecureANNSystem now handles key rotation path per test dir
    }

    @Test
    public void simpleEndToEndNearestNeighbor(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data2d.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true, \"opsThreshold\":2147483647, \"ageThresholdMs\":9223372036854775807}");

        Path keys = tempDir.resolve("keys.ser");

        List<Integer> dimensions = Arrays.asList(2);
        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir, false
        );

        int indexedCount = localSys.getIndexedVectorCount(2);
        logger.info("Indexed vectors for dim=2: {}", indexedCount);
        assertTrue(indexedCount > 0, "Should have at least 1 vector indexed, got: " + indexedCount);

        logger.info("Executing query for vector: [0.05, 0.05]");
        List<QueryResult> res = localSys.query(new double[]{0.05, 0.05}, 1, 2);

        assertEquals(1, res.size(), "Should return exactly 1 result");

        double expectedDist = Math.hypot(0.05 - 0.1, 0.05 - 0.1);
        double actualDist = res.get(0).getDistance();

        assertTrue(Math.abs(actualDist - expectedDist) < 0.05, "Expected distance ~" + expectedDist + " but got: " + actualDist);

        localSys.shutdown();
    }

//    @Test
//    public void testMultipleDimensions(@TempDir Path tempDir) throws Exception {
//        Path configFile = tempDir.resolve("config.json");
//        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");
//
//        Path keys = tempDir.resolve("keys.ser");
//
//        List<Integer> dimensions = Arrays.asList(2, 3);
//        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
//                configFile.toString(),
//                "", // dataPath unused since we will insert manually
//                keys.toString(),
//                dimensions, tempDir
//        );
//
//        // --- 2D data ---
//        for (int i = 0; i < 5; i++) {
//            double[] vec2D = new double[] { Math.random(), Math.random() };
//            localSys.insert(UUID.randomUUID().toString(), vec2D, 2);
//        }
//        int count2D = localSys.getIndexedVectorCount(2);
//        logger.info("Indexed 2D vectors: {}", count2D);
//        assertTrue(count2D > 0);
//
//        // --- 3D data ---
//        for (int i = 0; i < 5; i++) {
//            double[] vec3D = new double[] { Math.random(), Math.random(), Math.random() };
//            localSys.insert(UUID.randomUUID().toString(), vec3D, 3);
//        }
//        int count3D = localSys.getIndexedVectorCount(3);
//        logger.info("Indexed 3D vectors: {}", count3D);
//        assertTrue(count3D > 0);
//
//        // --- Queries ---
//
//        // 2D query
//        double[] query2D = new double[] { 0.1, 0.1 };
//        List<QueryResult> res2D = localSys.query(query2D, 1, 2);
//        assertTrue(res2D.size() >= 1);
//        logger.info("2D query result: {}", res2D.get(0).getDistance());
//
//        // 3D query
//        double[] query3D = new double[] { 0.1, 0.1, 0.1 };
//        List<QueryResult> res3D = localSys.query(query3D, 1, 3);
//        assertTrue(res3D.size() >= 1);
//        logger.info("3D query result: {}", res3D.get(0).getDistance());
//    }

    @Test
    public void testVisualization(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data2d.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");

        List<Integer> dimensions = Arrays.asList(2);
        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir, false
        );

        int indexedCount = localSys.getIndexedVectorCount(2);
        logger.info("Indexed vectors for dim=2: {}", indexedCount);
        assertTrue(indexedCount > 0, "Should have at least 1 vector indexed, got: " + indexedCount);

        try {
            double[] queryVector = new double[]{0.05, 0.05};
            localSys.runEndToEnd(dataFile.toString(), queryVector, 1, 2);
        } catch (Exception e) {
            logger.error("Visualization failed", e);
            throw e;
        }

        localSys.shutdown();
    }
}
