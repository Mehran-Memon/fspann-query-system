package java.com.fspann.index;

import java.com.fspann.encryption.EncryptionUtils;
import java.com.fspann.keymanagement.KeyManager;
import java.com.fspann.query.EncryptedPoint;
import java.com.fspann.utils.PersistenceUtils;
import java.com.fspann.query.QueryToken;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;

public class SecureLSHIndex {
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

        // Initialize the hash tables
        for (int i = 0; i < numHashTables; i++) {
            hashTables.add(new HashMap<>());
        }
    }

    // Save the encrypted index to disk
    public void saveIndex(String directoryPath) {
        try {
            // Save the encrypted points and hash tables to files
            PersistenceUtils.saveObject(encryptedPoints, directoryPath + "/encrypted_points.ser");
            PersistenceUtils.saveObject(hashTables, directoryPath + "/hash_tables.ser");
        } catch (IOException e) {
            e.printStackTrace();
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
    public int add(String id, double[] vector, boolean useFakePoints) throws Exception {
        // Encrypt the vector with the current key
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_v" + currentKey.hashCode(), id);
        encryptedPoints.put(id, encryptedPoint);

        // Add the point to the hash tables (this is a simplified version)
        int totalFakePointsAdded = 0;
        EvenLSH lsh = new EvenLSH(128, 10, Arrays.asList(vector)); // Dummy LSH
        int bucketId = lsh.getBucketId(vector);
        for (int i = 0; i < numHashTables; i++) {
            Map<Integer, List<EncryptedPoint>> table = hashTables.get(i);
            table.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(encryptedPoint);

            // Simulate adding fake points if necessary
            if (useFakePoints) {
                totalFakePointsAdded++;
            }
        }

        return totalFakePointsAdded;
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
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            return new ArrayList<>();
        }

        // Process candidate buckets and collect encrypted points
        for (int i = 0; i < numHashTables; i++) {
            for (Integer bucketId : candidateBuckets) {
                List<EncryptedPoint> bucket = hashTables.get(i).getOrDefault(bucketId, new ArrayList<>());
                candidates.addAll(bucket);
            }
        }

        // Return a list of encrypted points
        return new ArrayList<>(candidates);
    }


    // Rehash the index with a new key
    public void rehash(KeyManager keyManager, String context) throws Exception {
        SecretKey oldKey = keyManager.getPreviousKey();
        SecretKey newKey = keyManager.getCurrentKey();
        if (newKey == null) {
            throw new IllegalStateException("No current key available for rehashing");
        }

        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            for (List<EncryptedPoint> bucket : table.values()) {
                for (EncryptedPoint point : bucket) {
                    byte[] encryptedData = point.getCiphertext();
                    byte[] newEncryptedData = EncryptionUtils.reEncryptData(encryptedData, oldKey, newKey);
                    point.setCiphertext(newEncryptedData);
                }
            }
        }

        this.currentKey = newKey;
    }
}
