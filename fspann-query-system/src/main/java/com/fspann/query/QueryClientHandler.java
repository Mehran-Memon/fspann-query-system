package com.fspann.query;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;

import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class QueryClientHandler {

    private KeyManager keyManager;

    public QueryClientHandler(KeyManager keyManager) {
        this.keyManager = keyManager;
    }

    /**
     * Decrypt the candidate points, compute distances, and pick topK.
     */
    public List<double[]> decryptAndRefine(List<EncryptedPoint> encryptedCandidates, double[] queryVector, int topK) {
        List<ScoredPoint> scored = new ArrayList<>();
        try {
            // Retrieve the session key used to encrypt data in the relevant buckets
            SecretKey sessionKey = keyManager.getSessionKey("some-bucket-session"); 
            // In a real system, each bucket might have its own key

            for (EncryptedPoint encPoint : encryptedCandidates) {
                byte[] decrypted = EncryptionUtils.decrypt(encPoint.getCiphertext(), sessionKey);
                double[] point = bytesToVector(decrypted);

                // Compute distance to queryVector (e.g., Euclidean)
                double dist = euclideanDistance(queryVector, point);
                scored.add(new ScoredPoint(point, dist));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort by distance ascending
        Collections.sort(scored, Comparator.comparingDouble(ScoredPoint::distance));

        // Return topK actual vectors
        List<double[]> topKPoints = new ArrayList<>();
        for (int i = 0; i < Math.min(topK, scored.size()); i++) {
            topKPoints.add(scored.get(i).point());
        }
        return topKPoints;
    }

    private double[] bytesToVector(byte[] bytes) {
        // Inverse of vectorToBytes(...) in QueryGenerator
        // Suppose each double is 8 bytes
        int dim = bytes.length / 8;
        double[] vec = new double[dim];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < dim; i++) {
            vec[i] = buffer.getDouble();
        }
        return vec;
    }

    private double euclideanDistance(double[] q, double[] p) {
        double sum = 0.0;
        for (int i = 0; i < q.length; i++) {
            double diff = q[i] - p[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    // A small record to store point + distance
    private static record ScoredPoint(double[] point, double distance) {}
}
