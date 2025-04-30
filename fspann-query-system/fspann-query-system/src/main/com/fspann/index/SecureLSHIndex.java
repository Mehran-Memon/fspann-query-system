package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.utils.PersistenceUtils;
import com.fspann.query.QueryToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;

public class SecureLSHIndex {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);
    private final List<Map<Integer, List<EncryptedPoint>>> hashTables;
    private final Map<String, EncryptedPoint> encryptedPoints;
    private SecretKey currentKey; // Current key for encryption and decryption
    private final int numHashTables;

    // Constructor initializes the hash tables and LSH functions
    public SecureLSHIndex(int numHashTables, SecretKey key, List<double[]> initialData) {
        this.numHashTables = numHashTables;
        this.currentKey = key;
        this.hashTables = new ArrayList<>();
        this.encryptedPoints = new HashMap<>();
        for (int i = 0; i < numHashTables; i++) {
            hashTables.add(new HashMap<>());
        }
        // Build the index using initial data
        if (initialData != null && !initialData.isEmpty()) {
            addInitialData(initialData); // Calls addInitialData
        }
    }

    // Add initial data to the index (for first time setup or loading dataset)
    private void addInitialData(List<double[]> initialData) {
        for (double[] vector : initialData) {
            try {
                add(UUID.randomUUID().toString(), vector, false, initialData);
            } catch (Exception e) {
                logger.error("Error adding vector: {}", e.getMessage(), e);
            }
        }
    }

    // Save the encrypted index to disk
    public void saveIndex(String directoryPath) {
        try {
            PersistenceUtils.saveObject(encryptedPoints, directoryPath + "/encrypted_points.ser");
            PersistenceUtils.saveObject(hashTables, directoryPath + "/hash_tables.ser");
            logger.info("Index saved to: {}", directoryPath);
        } catch (IOException e) {
            logger.error("Failed to save index to: {}", directoryPath, e);
            throw new RuntimeException("Failed to save index", e);
        }
    }

    // Load the encrypted index from disk
    public void loadIndex(String directoryPath) {
        try {
            // Load the encrypted points and hash tables from files
            Map<String, EncryptedPoint> epMap = PersistenceUtils.loadObject(directoryPath + "/encrypted_points.ser", Map.class);
            List<Map<Integer, List<EncryptedPoint>>> htList = PersistenceUtils.loadObject(directoryPath + "/hash_tables.ser", List.class);

            this.encryptedPoints.putAll(epMap);
            this.hashTables.addAll(htList);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Add a new encrypted vector to the index
    public int add(String id, double[] vector, boolean useFakePoints, List<double[]> baseVectors) throws Exception {
        int dimension = vector.length;
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        int index = baseVectors.indexOf(vector);
        if (index == -1) {
            logger.error("Vector not found in baseVectors for id: {}", id);
            throw new IllegalArgumentException("Vector not found in baseVectors");
        }
        EvenLSH lsh = new EvenLSH(dimension, 10);
        int bucketId = lsh.getBucketId(vector);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_v" + bucketId, id, index);
        encryptedPoints.put(id, encryptedPoint);
        int totalFakePointsAdded = 0;
        for (int i = 0; i < numHashTables; i++) {
            Map<Integer, List<EncryptedPoint>> table = hashTables.get(i);
            table.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(encryptedPoint);
            if (useFakePoints) {
                totalFakePointsAdded++;
            }
        }
        return totalFakePointsAdded;
    }

    private int findVectorIndex(double[] vector, List<double[]> baseVectors) {
        for (int i = 0; i < baseVectors.size(); i++) {
            if (Arrays.equals(vector, baseVectors.get(i))) {
                return i;
            }
        }
        return -1;
    }

    // Remove a point by ID from the index
    public void remove(String id) {
        encryptedPoints.remove(id);

        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            table.forEach((bucketId, points) -> points.removeIf(p -> p.getPointId().equals(id)));
        }
    }

    // Retrieve the list of LSH functions (just a placeholder for now)
    public List<EvenLSH> getLshFunctions() {
        // Return a dummy list (In practice, this would return actual LSH functions)
        return new ArrayList<>();
    }

    // Set the current key (for rehashing)
    public void setCurrentKey(SecretKey key) {
        this.currentKey = key;
    }

    // Find nearest neighbors using the encrypted query and LSH
    public List<EncryptedPoint> findNearestNeighborsEncrypted(QueryToken queryToken) {
        if (queryToken == null) {
            throw new IllegalArgumentException("QueryToken cannot be null");
        }
        Set<EncryptedPoint> candidates = new HashSet<>();
        List<Integer> candidateBuckets = queryToken.getCandidateBuckets();
        int numTables = queryToken.getNumTables(); // Use numTables from QueryToken
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            return new ArrayList<>();
        }
        for (int i = 0; i < Math.min(numTables, numHashTables); i++) {
            for (Integer bucketId : candidateBuckets) {
                List<EncryptedPoint> bucket = hashTables.get(i).getOrDefault(bucketId, new ArrayList<>());
                candidates.addAll(bucket);
            }
        }
        List<EncryptedPoint> result = new ArrayList<>(candidates);
        return result.subList(0, Math.min(queryToken.getTopK(), result.size()));
    }

    // Rehash the index with a new key
    public void rehash(KeyManager keyManager, String context) throws Exception {
        SecretKey oldKey = keyManager.getPreviousKey();
        SecretKey newKey = keyManager.getCurrentKey();
        if (newKey == null) {
            throw new IllegalStateException("No current key available for rehashing");
        }

        // Log the context and rehashing operation
        logger.info("Rehashing with context: {}", context);

        // Re-encrypt all data with the new key
        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            for (List<EncryptedPoint> bucket : table.values()) {
                for (EncryptedPoint point : bucket) {
                    byte[] encryptedData = point.getCiphertext();
                    byte[] newEncryptedData = EncryptionUtils.reEncryptData(encryptedData, oldKey, newKey);
                    point.setCiphertext(newEncryptedData);  // Update the encrypted data with the new key
                }
            }
        }

        this.currentKey = newKey;  // Update the current key
    }
}
