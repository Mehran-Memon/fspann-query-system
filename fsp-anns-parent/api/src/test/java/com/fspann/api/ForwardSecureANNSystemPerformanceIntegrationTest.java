package com.fspann.api;

import com.fspann.common.QueryResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecureANNSystemPerformanceIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemPerformanceIntegrationTest.class);
    private static ForwardSecureANNSystem sys;
    private static List<double[]> dataset;
    private static final int DIMS = 10;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) throws Exception {
        // Paths for configuration, data, and keys
        Path cfg = tempDir.resolve("config.json");
        Files.writeString(cfg, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path data = tempDir.resolve("data.csv");
        StringBuilder sb = new StringBuilder();
        dataset = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            double[] vector = new double[DIMS];
            for (int j = 0; j < DIMS; j++) {
                vector[j] = Math.random();
            }
            dataset.add(vector);
            for (int j = 0; j < DIMS; j++) {
                sb.append(vector[j]);
                if (j < DIMS - 1) sb.append(',');
            }
            sb.append('\n');
        }
        Files.writeString(data, sb.toString());

        Path keys = tempDir.resolve("keys.ser");

        // Path to metadata
        Path metadataPath = tempDir.resolve("metadata");

        // Initialize ForwardSecureANNSystem with updated constructor
        sys = new ForwardSecureANNSystem(
                cfg.toString(),
                data.toString(),
                keys.toString(),
                Arrays.asList(DIMS),
                metadataPath  // Pass the correct Path for metadata
        );
    }

    @AfterAll
    public static void tearDown() {
        if (sys != null) {
            sys.shutdown();
        }
    }

    @Test
    public void bulkPerformanceTest() throws Exception {
        assertNotNull(dataset, "Dataset should not be null");
        assertTrue(dataset.size() > 0, "Dataset should contain data");

        int inserts = 1000;
        long start = System.nanoTime();
        for (int i = 0; i < inserts; i++) {
            String vectorId = UUID.randomUUID().toString();
            logger.debug("Inserting vector with ID: {}", vectorId);
            sys.insert(vectorId, dataset.get(i % dataset.size()), DIMS);
        }
        long endTime = System.nanoTime();
        double avgMs = (endTime - start) / 1e6 / inserts;
        logger.info("Average insert latency: {:.3f} ms", avgMs);
        assertTrue(avgMs < 50, "Insert too slow: " + avgMs + " ms");

        int queries = 200;
        start = System.nanoTime();
        for (int i = 0; i < queries; i++) {
            logger.debug("Querying for vector: {}", Arrays.toString(dataset.get(i % dataset.size())));
            sys.queryWithCloak(dataset.get(i % dataset.size()), 5, DIMS);
        }
        endTime = System.nanoTime();
        avgMs = (endTime - start) / 1e6 / queries;
        logger.info("Average query latency: {:.3f} ms", avgMs);
        assertTrue(avgMs < 100, "Query too slow: " + avgMs + " ms");
    }

    @Test
    public void endToEndWorkflowTest(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < DIMS; j++) {
                sb.append(i * 0.1).append(j == DIMS - 1 ? "\n" : ",");
            }
        }
        Files.writeString(dataFile, sb.toString());

        double[] queryVector = new double[DIMS];
        for (int i = 0; i < DIMS; i++) {
            queryVector[i] = 0.05 + (i * 0.01);
        }
        sys.runEndToEnd(dataFile.toString(), queryVector, 1, DIMS);

        List<QueryResult> res = sys.queryWithCloak(queryVector, 1, DIMS);
        assertEquals(1, res.size(), "Should return 1 result");
        double expectedDist = Math.sqrt(DIMS * Math.pow(0.05 - 0.1, 2));
        assertEquals(expectedDist, res.get(0).getDistance(), 0.1, "Distance should be close to expected");
    }

    @Test
    public void testFakePointsInsertion(@TempDir Path tempDir) throws Exception {
        int numFakePoints = 100;
        sys.insertFakePoints(numFakePoints, DIMS);
        assertTrue(sys.getIndexedVectorCount(DIMS) >= numFakePoints, "Should have at least " + numFakePoints + " vectors indexed after fake points insertion");
    }
}
