package com.fspann.api;

import com.fspann.common.QueryResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
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
        Path cfg = tempDir.resolve("config.json");
        Files.writeString(cfg, "{\"numShards\":4, \"profilerEnabled\":true}");
        logger.debug("Created config file: {}", cfg);

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
        logger.debug("Created data file: {} with {} vectors", data, dataset.size());

        Path keys = tempDir.resolve("keys.ser");
        sys = new ForwardSecureANNSystem(cfg.toString(), data.toString(), keys.toString(), Arrays.asList(DIMS), tempDir);
        logger.info("Initialized system with {} vectors indexed for dim={}", sys.getIndexedVectorCount(DIMS), DIMS);
    }

    @AfterAll
    public static void tearDown() {
        if (sys != null) {
            logger.info("Shutting down system");
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
        logger.info("Average insert latency: {} ms", avgMs);
        assertTrue(avgMs < 2000, "Insert too slow: " + avgMs + " ms");

        int queries = 200;
        start = System.nanoTime();
        for (int i = 0; i < queries; i++) {
            logger.debug("Querying for vector: {}", Arrays.toString(dataset.get(i % dataset.size())));
            sys.queryWithCloak(dataset.get(i % dataset.size()), 5, DIMS);
        }
        endTime = System.nanoTime();
        double avgMsQuery = (endTime - start) / 1e6 / queries;
        logger.info("Average query latency: {} ms", avgMsQuery);
        assertTrue(avgMsQuery < 100, "Query too slow: " + avgMsQuery + " ms");
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

        sys.runEndToEnd(dataFile.toString(), queryVector, 5, DIMS);

        List<QueryResult> res = sys.query(queryVector, 5, DIMS);
        logger.info("Query returned {} results:", res.size());
        for (QueryResult r : res) {
            logger.info("ID: {}, Distance: {}", r.getId(), r.getDistance());
        }

        // Just basic assertion to ensure results exist
        assertTrue(res.size() > 0, "Should return at least 1 result");
    }

    @Test
    public void testFakePointsInsertion(@TempDir Path tempDir) throws Exception {
        int numFakePoints = 100;
        sys.insertFakePoints(numFakePoints, DIMS);
        int indexedCount = sys.getIndexedVectorCount(DIMS);
        logger.info("Indexed vectors after fake points: {}", indexedCount);
        assertTrue(indexedCount >= numFakePoints, "Should have at least " + numFakePoints + " vectors indexed, got: " + indexedCount);
    }
}
