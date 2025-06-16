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
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecureANNSystemPerformanceIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemPerformanceIntegrationTest.class);
    private static ForwardSecureANNSystem sys;
    private static List<double[]> dataset;
    private static int dims;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) throws Exception {
        Path cfg = tempDir.resolve("config.json");
        Files.writeString(cfg, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path data = tempDir.resolve("data.csv");
        dims = 10;
        StringBuilder sb = new StringBuilder();
        dataset = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            double[] vector = new double[dims];
            for (int j = 0; j < dims; j++) {
                vector[j] = Math.random();
            }
            dataset.add(vector);
            for (int j = 0; j < dims; j++) {
                sb.append(vector[j]);
                if (j < dims - 1) sb.append(',');
            }
            sb.append('\n');
        }
        Files.writeString(data, sb.toString());

        Path keys = tempDir.resolve("keys.ser");
        sys = new ForwardSecureANNSystem(cfg.toString(), data.toString(), keys.toString(), dims);
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
            sys.insert(vectorId, dataset.get(i % dataset.size()));
        }
        long endTime = System.nanoTime();
        double avgMs = (endTime - start) / 1e6 / inserts;
        logger.info("Average insert latency: {:.3f} ms", avgMs);

        int queries = 200;
        start = System.nanoTime();
        for (int i = 0; i < queries; i++) {
            logger.debug("Querying for vector: {}", dataset.get(i % dataset.size()));
            sys.queryWithCloak(dataset.get(i % dataset.size()), 5);
        }
        endTime = System.nanoTime();
        avgMs = (endTime - start) / 1e6 / queries;
        logger.info("Average query latency: {:.3f} ms", avgMs);

        assertTrue(avgMs < 100, "Query too slow: " + avgMs + " ms");
    }

    @Test
    public void endToEndWorkflowTest(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        // Use 10D data to match setup
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < dims; j++) {
                sb.append(i * 0.1).append(j == dims - 1 ? "\n" : ",");
            }
        }
        Files.writeString(dataFile, sb.toString());

        double[] queryVector = new double[dims];
        for (int i = 0; i < dims; i++) {
            queryVector[i] = 0.05 + (i * 0.01);
        }
        sys.runEndToEnd(dataFile.toString(), queryVector, 1);

        List<QueryResult> res = sys.queryWithCloak(queryVector, 1);
        assertEquals(1, res.size());
        // Approximate distance check for 10D space
        double expectedDist = Math.sqrt(dims * 0.005 * 0.005); // Rough estimate
        assertEquals(expectedDist, res.get(0).getDistance(), 0.1);
    }
}