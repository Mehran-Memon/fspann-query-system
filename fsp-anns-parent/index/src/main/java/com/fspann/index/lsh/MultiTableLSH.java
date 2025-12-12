package com.fspann.index.lsh;

import java.util.*;
import java.util.concurrent.*;

/**
 * MultiTableLSH – Multi-table Locality-Sensitive Hashing Index
 *
 * Core properties:
 *  • L independent hash tables (multi-table design)
 *  • K hash functions per table
 *  • Adaptive bucket width tuning
 *  • Thread-safe concurrent operations
 *  • Distance-based candidate ranking
 *  • Metrics tracking for performance monitoring
 *  • Compatible with SecureLSHIndexService
 *
 * Key methods:
 *  • insert(id, vector) - Index a vector with given ID
 *  • query(query, topK) - Find top-K nearest candidates
 *  • adaptiveQuery(query, topK) - Query with automatic tuning
 *  • getAverageCandidateRatio(topK) - Monitor ratio metric
 *
 * @author FSP-ANNS Project
 * @version 2.0 (String IDs)
 */
public class MultiTableLSH {

    private final int numTables;
    private final int numFunctions;
    private final int numBuckets;
    private final RandomProjectionLSH hashFamily;
    private final AdaptiveProbeScheduler scheduler;

    // Storage: tables[tableId][bucketId] = list of (vectorId, vector)
    private final List<ConcurrentHashMap<Integer, List<StoredVector>>> tables;

    // Vector storage by ID
    private final ConcurrentHashMap<String, double[]> vectorStore;

    // Metrics
    private final QueryMetrics metrics;

    // Initialized flag
    private volatile boolean initialized = false;
    private int dimension = -1;

    /**
     * Construct MultiTableLSH index.
     *
     * @param numTables    L (number of hash tables)
     * @param numFunctions K (hash functions per table)
     * @param numBuckets   number of buckets per table
     */
    public MultiTableLSH(int numTables, int numFunctions, int numBuckets) {
        this.numTables = numTables;
        this.numFunctions = numFunctions;
        this.numBuckets = numBuckets;
        this.hashFamily = new RandomProjectionLSH();
        this.tables = new ArrayList<>();
        this.vectorStore = new ConcurrentHashMap<>();
        this.metrics = new QueryMetrics();
        this.scheduler = new AdaptiveProbeScheduler(this, this.hashFamily, 10);
    }

    /**
     * Initialize LSH with hash family.
     * Must be called before insert/query operations.
     */
    public void init(int dimension, RandomProjectionLSH hashFamily) {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }

        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be > 0");
        }

        this.dimension = dimension;

        // Initialize hash family
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);

        // Initialize tables
        for (int t = 0; t < numTables; t++) {
            ConcurrentHashMap<Integer, List<StoredVector>> table =
                    new ConcurrentHashMap<>();
            tables.add(table);
        }

        this.initialized = true;
    }

    /**
     * Insert vector into index.
     *
     * @param id       unique vector identifier (String)
     * @param vector   vector to index
     * @throws IllegalStateException if not initialized
     * @throws IllegalArgumentException if vector dimension mismatches
     */
    public void insert(String id, double[] vector) {
        if (!initialized) {
            throw new IllegalStateException("Not initialized. Call init() first.");
        }
        if (vector == null || vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch. Expected: " + dimension +
                            ", got: " + (vector == null ? "null" : vector.length));
        }
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("ID cannot be null or empty");
        }

        // Store vector
        vectorStore.put(id, vector.clone());

        // Hash and insert into all tables
        for (int t = 0; t < numTables; t++) {
            int bucketId = hashFamily.hash(vector, t, 0);

            List<StoredVector> bucket =
                    tables.get(t).computeIfAbsent(bucketId, k -> new CopyOnWriteArrayList<>());
            bucket.add(new StoredVector(id, vector));
        }
    }

    /**
     * Query for top-K nearest neighbors.
     *
     * @param query query vector
     * @param topK  number of results to return
     * @return sorted list of (vectorId, distance) pairs
     */
    public List<Map.Entry<String, Double>> query(double[] query, int topK) {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        if (query == null || query.length != dimension) {
            throw new IllegalArgumentException("Query dimension mismatch");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be > 0");
        }

        long startTime = System.nanoTime();

        Set<String> candidates = new HashSet<>();

        // Collect candidates from all tables
        for (int t = 0; t < numTables; t++) {
            int bucketId = hashFamily.hash(query, t, 0);
            List<StoredVector> bucket = tables.get(t).get(bucketId);

            if (bucket != null) {
                for (StoredVector sv : bucket) {
                    candidates.add(sv.id);
                }
            }
        }

        // Compute distances and rank
        List<Map.Entry<String, Double>> ranked = new ArrayList<>();
        for (String candId : candidates) {
            double[] candVec = vectorStore.get(candId);
            if (candVec != null) {
                double dist = euclideanDistance(query, candVec);
                ranked.add(new AbstractMap.SimpleEntry<>(candId, dist));
            }
        }

        ranked.sort((a, b) -> Double.compare(a.getValue(), b.getValue()));

        List<Map.Entry<String, Double>> result =
                ranked.subList(0, Math.min(topK, ranked.size()));

        long elapsed = (System.nanoTime() - startTime) / 1_000_000; // ms

        metrics.recordQuery(candidates.size(), topK, elapsed);

        return new ArrayList<>(result);
    }

    /**
     * Query with adaptive parameter tuning.
     */
    public List<Map.Entry<String, Double>> adaptiveQuery(double[] query, int topK) {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        return scheduler.adaptiveQuery(query);
    }

    /**
     * Query with adaptive tuning (simplified).
     */
    public List<Map.Entry<String, Double>> adaptiveQuery(double[] query) {
        return adaptiveQuery(query, 10);
    }

    /**
     * Get average candidate ratio for given topK.
     */
    public double getAverageCandidateRatio(int topK) {
        return metrics.getAverageRatio(topK);
    }

    /**
     * Get query statistics.
     */
    public Map<String, Double> getQueryStatistics(int topK) {
        return metrics.getStatistics(topK);
    }

    /**
     * Get total vectors indexed.
     */
    public int getTotalVectorsIndexed() {
        return vectorStore.size();
    }

    /**
     * Get bucket count for a table.
     */
    public int getTableBucketCount(int tableId) {
        if (tableId < 0 || tableId >= numTables) {
            throw new IllegalArgumentException("Invalid table ID");
        }
        return tables.get(tableId).size();
    }

    /**
     * Clear all data.
     */
    public void clear() {
        vectorStore.clear();
        for (ConcurrentHashMap<Integer, List<StoredVector>> table : tables) {
            table.clear();
        }
        metrics.clear();
    }

    /**
     * Get vector by ID.
     */
    public double[] getVector(String id) {
        double[] vec = vectorStore.get(id);
        return vec != null ? vec.clone() : null;
    }

    @Override
    public String toString() {
        return String.format(
                "MultiTableLSH{dim=%d, indexed=%d, L=%d, K=%d}",
                dimension, getTotalVectorsIndexed(), numTables, numFunctions);
    }

    // ====================================================================
    // PRIVATE HELPERS
    // ====================================================================

    private double euclideanDistance(double[] a, double[] b) {
        if (a == null || b == null || a.length != b.length) {
            return Double.MAX_VALUE;
        }

        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    // ====================================================================
    // INNER CLASSES
    // ====================================================================

    /**
     * Stored vector with ID.
     */
    private static class StoredVector {
        final String id;
        final double[] vector;

        StoredVector(String id, double[] vector) {
            this.id = id;
            this.vector = vector;
        }
    }

    /**
     * Query metrics tracking.
     */
    private static class QueryMetrics {
        private final ConcurrentHashMap<Integer, List<QueryResult>> resultsByTopK =
                new ConcurrentHashMap<>();

        void recordQuery(int candidatesRetrieved, int topK, long latencyMs) {
            resultsByTopK.computeIfAbsent(topK, k -> new CopyOnWriteArrayList<>())
                    .add(new QueryResult(candidatesRetrieved, topK, latencyMs));

            // Keep only last 100 results per topK
            List<QueryResult> results = resultsByTopK.get(topK);
            if (results.size() > 100) {
                results.remove(0);
            }
        }

        double getAverageRatio(int topK) {
            List<QueryResult> results = resultsByTopK.get(topK);
            if (results == null || results.isEmpty()) {
                return 1.0;
            }

            double sumRatio = 0;
            for (QueryResult qr : results) {
                sumRatio += (double) qr.candidatesRetrieved / topK;
            }
            return sumRatio / results.size();
        }

        Map<String, Double> getStatistics(int topK) {
            List<QueryResult> results = resultsByTopK.get(topK);
            if (results == null || results.isEmpty()) {
                return Map.of(
                        "count", 0.0,
                        "avg_ratio", 1.0,
                        "avg_latency_ms", 0.0
                );
            }

            double sumRatio = 0;
            double sumLatency = 0;

            for (QueryResult qr : results) {
                sumRatio += (double) qr.candidatesRetrieved / topK;
                sumLatency += qr.latencyMs;
            }

            int count = results.size();
            return Map.of(
                    "count", (double) count,
                    "avg_ratio", sumRatio / count,
                    "avg_latency_ms", sumLatency / count
            );
        }

        void clear() {
            resultsByTopK.clear();
        }

        private static class QueryResult {
            final int candidatesRetrieved;
            final int topK;
            final long latencyMs;

            QueryResult(int candidatesRetrieved, int topK, long latencyMs) {
                this.candidatesRetrieved = candidatesRetrieved;
                this.topK = topK;
                this.latencyMs = latencyMs;
            }
        }
    }
}