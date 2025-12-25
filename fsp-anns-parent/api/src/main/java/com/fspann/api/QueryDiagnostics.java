package com.fspann.api;

import com.fspann.common.QueryResult;
import com.fspann.loader.GroundtruthManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

/**
 * QueryDiagnostics - Runtime validation and debugging
 *
 * This class provides hooks to diagnose precision = 0 issues at runtime.
 * It logs detailed information about:
 *   - Returned IDs vs GT IDs
 *   - Distance values
 *   - Code matching behavior
 */
public final class QueryDiagnostics {

    private static final Logger logger = LoggerFactory.getLogger(QueryDiagnostics.class);

    // Track worst queries for analysis
    private final PriorityQueue<DiagnosticEntry> worstQueries;
    private final int worstKeep;
    private int totalQueries = 0;
    private int zeroHitQueries = 0;
    private boolean failOnZeroHit;

    // Output file for detailed diagnostics
    private final Path outputPath;

    public QueryDiagnostics(Path outputDir, int worstKeep, boolean failOnZeroHit) throws IOException {
        this.worstKeep = worstKeep;
        this.failOnZeroHit = failOnZeroHit;
        this.worstQueries = new PriorityQueue<>(
                Comparator.comparingDouble(DiagnosticEntry::precision).reversed()
        );

        Files.createDirectories(outputDir);
        this.outputPath = outputDir.resolve("query_diagnostics.csv");

        // Write header
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            w.println("queryIndex,k,precision,returnedIds,gtIds,returnedDistances,gtDistances");
        }
    }

    /**
     * Record a query result for diagnostics.
     *
     * @param queryIndex query index (0-based)
     * @param k number of neighbors requested
     * @param results returned results
     * @param gt groundtruth manager
     * @param queryVector the query vector (for distance computation)
     * @param baseReader optional reader for computing distances
     */
    public void record(
            int queryIndex,
            int k,
            List<QueryResult> results,
            GroundtruthManager gt,
            double[] queryVector,
            BaseDistanceReader baseReader
    ) {
        totalQueries++;

        // Get GT IDs
        int[] gtIds = gt.getGroundtruthIds(queryIndex, k);
        Set<Integer> gtSet = new HashSet<>();
        if (gtIds != null) {
            for (int id : gtIds) gtSet.add(id);
        }

        // Compute hits
        List<Integer> returnedIds = new ArrayList<>();
        int hits = 0;

        for (QueryResult r : results) {
            try {
                int id = Integer.parseInt(r.getId());
                returnedIds.add(id);
                if (gtSet.contains(id)) hits++;
            } catch (NumberFormatException e) {
                logger.warn("Non-integer ID in results: {}", r.getId());
            }
        }

        double precision = (k > 0) ? (double) hits / k : 0.0;

        // Check for zero hits
        if (hits == 0 && !results.isEmpty()) {
            zeroHitQueries++;

            // Log detailed diagnostics for first few zero-hit queries
            if (zeroHitQueries <= 10) {
                logZeroHitQuery(queryIndex, k, returnedIds, gtIds, results, baseReader, queryVector);
            }

            // Fail fast if configured
            if (failOnZeroHit && totalQueries >= 5) {
                double zeroRate = (double) zeroHitQueries / totalQueries;
                if (zeroRate > 0.8) {
                    throw new RuntimeException(
                            String.format(
                                    "CRITICAL: %.1f%% of queries have zero GT hits! " +
                                            "System is not returning correct neighbors. " +
                                            "Check: 1) GT file, 2) ID consistency, 3) Code generation",
                                    zeroRate * 100
                            )
                    );
                }
            }
        }

        // Track worst queries
        DiagnosticEntry entry = new DiagnosticEntry(
                queryIndex, k, precision, returnedIds,
                gtIds != null ? gtIds : new int[0]
        );
        worstQueries.offer(entry);
        if (worstQueries.size() > worstKeep) {
            worstQueries.poll();
        }

        // Write to CSV
        writeToCSV(entry, results, baseReader, queryVector);
    }

    /**
     * Log detailed diagnostics for a zero-hit query.
     */
    private void logZeroHitQuery(
            int queryIndex,
            int k,
            List<Integer> returnedIds,
            int[] gtIds,
            List<QueryResult> results,
            BaseDistanceReader baseReader,
            double[] queryVector
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== ZERO-HIT QUERY DIAGNOSTIC ==========\n");
        sb.append(String.format("Query Index: %d, K: %d\n", queryIndex, k));
        sb.append(String.format("Returned IDs (%d): %s\n",
                returnedIds.size(),
                returnedIds.stream().limit(20).map(Object::toString).collect(Collectors.joining(", "))
        ));

        if (gtIds != null && gtIds.length > 0) {
            sb.append(String.format("GT IDs (%d): %s\n",
                    gtIds.length,
                    Arrays.stream(gtIds).limit(20).mapToObj(Integer::toString).collect(Collectors.joining(", "))
            ));

            // Check for overlap
            Set<Integer> returnedSet = new HashSet<>(returnedIds);
            Set<Integer> gtSet = new HashSet<>();
            for (int id : gtIds) gtSet.add(id);

            Set<Integer> intersection = new HashSet<>(returnedSet);
            intersection.retainAll(gtSet);

            sb.append(String.format("Overlap: %d IDs\n", intersection.size()));

            // Check ID ranges
            int minReturned = returnedIds.stream().mapToInt(i -> i).min().orElse(-1);
            int maxReturned = returnedIds.stream().mapToInt(i -> i).max().orElse(-1);
            int minGt = Arrays.stream(gtIds).min().orElse(-1);
            int maxGt = Arrays.stream(gtIds).max().orElse(-1);

            sb.append(String.format("Returned ID range: [%d, %d]\n", minReturned, maxReturned));
            sb.append(String.format("GT ID range: [%d, %d]\n", minGt, maxGt));

            // Distance comparison
            if (baseReader != null && queryVector != null) {
                try {
                    // Get distances to returned IDs
                    sb.append("\nReturned distances (first 5):\n");
                    for (int i = 0; i < Math.min(5, results.size()); i++) {
                        QueryResult r = results.get(i);
                        int id = Integer.parseInt(r.getId());
                        double storedDist = r.getDistance();
                        double computedDist = baseReader.l2(queryVector, id);
                        sb.append(String.format("  ID %d: stored=%.4f, computed=%.4f\n",
                                id, storedDist, computedDist));
                    }

                    // Get distances to GT IDs
                    sb.append("\nGT distances (first 5):\n");
                    for (int i = 0; i < Math.min(5, gtIds.length); i++) {
                        int id = gtIds[i];
                        double dist = baseReader.l2(queryVector, id);
                        sb.append(String.format("  ID %d: dist=%.4f\n", id, dist));
                    }
                } catch (Exception e) {
                    sb.append("Error computing distances: ").append(e.getMessage()).append("\n");
                }
            }
        } else {
            sb.append("GT: NONE (groundtruth missing for this query)\n");
        }

        sb.append("================================================\n");
        logger.error(sb.toString());
    }

    private void writeToCSV(
            DiagnosticEntry entry,
            List<QueryResult> results,
            BaseDistanceReader baseReader,
            double[] queryVector
    ) {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(
                outputPath, StandardOpenOption.APPEND))) {

            String returnedIds = entry.returnedIds.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(";"));

            String gtIds = Arrays.stream(entry.gtIds)
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(";"));

            String returnedDists = results.stream()
                    .map(r -> String.format("%.4f", r.getDistance()))
                    .collect(Collectors.joining(";"));

            String gtDists = "";
            if (baseReader != null && queryVector != null) {
                gtDists = Arrays.stream(entry.gtIds)
                        .limit(10)
                        .mapToDouble(id -> {
                            try {
                                return baseReader.l2(queryVector, id);
                            } catch (Exception e) {
                                return -1.0;
                            }
                        })
                        .mapToObj(d -> String.format("%.4f", d))
                        .collect(Collectors.joining(";"));
            }

            w.printf("%d,%d,%.4f,\"%s\",\"%s\",\"%s\",\"%s\"%n",
                    entry.queryIndex, entry.k, entry.precision,
                    returnedIds, gtIds, returnedDists, gtDists);

        } catch (IOException e) {
            logger.warn("Failed to write diagnostics", e);
        }
    }

    /**
     * Get summary statistics.
     */
    public String getSummary() {
        double zeroRate = (totalQueries > 0)
                ? (double) zeroHitQueries / totalQueries * 100
                : 0.0;

        return String.format(
                "QueryDiagnostics: total=%d, zeroHit=%d (%.1f%%), tracked=%d worst",
                totalQueries, zeroHitQueries, zeroRate, worstQueries.size()
        );
    }

    /**
     * Get the worst queries (lowest precision).
     */
    public List<DiagnosticEntry> getWorstQueries() {
        return new ArrayList<>(worstQueries);
    }

    // =========================================================
    // HELPER CLASSES
    // =========================================================

    public static final class DiagnosticEntry {
        public final int queryIndex;
        public final int k;
        public final double precision;
        public final List<Integer> returnedIds;
        public final int[] gtIds;

        DiagnosticEntry(int queryIndex, int k, double precision,
                        List<Integer> returnedIds, int[] gtIds) {
            this.queryIndex = queryIndex;
            this.k = k;
            this.precision = precision;
            this.returnedIds = returnedIds;
            this.gtIds = gtIds;
        }

        double precision() { return precision; }
    }

    /**
     * Interface for reading distances from base vectors.
     */
    public interface BaseDistanceReader {
        double l2(double[] query, int vectorId);
    }
}
