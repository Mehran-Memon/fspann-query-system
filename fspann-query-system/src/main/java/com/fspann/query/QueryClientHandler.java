package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Handles client-side query refinement by decrypting candidates, computing distances, and selecting top-k nearest neighbors.
 */
public class QueryClientHandler {

    private final KeyManager keyManager;
    private final ExecutorService executorService;

    public QueryClientHandler(KeyManager keyManager) {
        this.keyManager = keyManager;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Decrypts candidate points, computes distances to the query vector, and returns the top-k nearest neighbors.
     * @param encryptedCandidates List of encrypted candidate points.
     * @param queryVector The query vector.
     * @param topK Number of nearest neighbors to return.
     * @return List of top-k nearest points (plaintext double[]).
     * @throws Exception If decryption or processing fails.
     */
    public List<double[]> decryptAndRefine(List<EncryptedPoint> encryptedCandidates, double[] queryVector, int topK) throws Exception {
        List<ScoredPoint> scored = new ArrayList<>();

        // Parallelize decryption and distance computation for large candidate sets
        List<ScoredPoint> results = encryptedCandidates.parallelStream()
                .map(candidate -> {
                    try {
                        // Retrieve the session key based on the bucket ID or epoch
                        String context = "epoch_" + keyManager.getTimeEpoch(); // Adjust context as needed
                        SecretKey sessionKey = keyManager.getSessionKey(context);
                        if (sessionKey == null) {
                            throw new IllegalStateException("No session key found for context: " + context);
                        }

                        // Decrypt the candidate point
                        double[] point = candidate.decrypt(sessionKey);

                        // Compute Euclidean distance to query vector
                        double dist = euclideanDistance(queryVector, point);
                        return new ScoredPoint(point, dist, candidate.getPointId());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to process candidate point " + candidate.getPointId() + ": " + e.getMessage(), e);
                    }
                })
                .collect(Collectors.toList());

        scored.addAll(results);

        // Sort by distance (ascending)
        scored.sort(Comparator.comparingDouble(ScoredPoint::distance));

        // Return top-k actual vectors
        List<double[]> topKPoints = new ArrayList<>();
        for (int i = 0; i < Math.min(topK, scored.size()); i++) {
            topKPoints.add(scored.get(i).point());
        }
        return topKPoints;
    }

    /**
     * Computes the Euclidean distance between two vectors.
     * @param v1 First vector.
     * @param v2 Second vector.
     * @return The Euclidean distance.
     */
    private double euclideanDistance(double[] v1, double[] v2) {
        if (v1.length != v2.length) {
            throw new IllegalArgumentException("Vector dimensions mismatch: " + v1.length + " vs " + v2.length);
        }
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Shuts down the executor service.
     */
    public void shutdown() {
        executorService.shutdown();
    }

    // Record to store point, distance, and point ID
    private static record ScoredPoint(double[] point, double distance, String pointId) {}
}