package com.fspann.index;

import com.fspann.config.SystemConfig;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.utils.PersistenceUtils;
import com.fspann.query.QueryToken;
import com.fspann.utils.Profiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;

public class SecureLSHIndex {

    /* ---------- constants ---------- */
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);
    private static final int    NUM_SHARDS = 32;      // <-- choose any shard fan-out

    /* ---------- fields ---------- */
    private final Map<String, EncryptedPoint> encryptedPoints = new HashMap<>();
    private final List<Map<Integer, List<EncryptedPoint>>> hashTables = new ArrayList<>();
    private final Map<Integer, Integer> bucketToShard = new HashMap<>();

    private SecretKey currentKey;
    private final int numHashTables;
    private final Profiler profiler;

    /* ---------- ctor ---------- */
    public SecureLSHIndex(int numHashTables,
                          SecretKey key,
                          List<double[]> initialData) {

        this.numHashTables = numHashTables;
        this.currentKey    = key;
        this.profiler = new Profiler();

        for (int i = 0; i < numHashTables; i++) {
            hashTables.add(new HashMap<>());
        }

        if (initialData != null && !initialData.isEmpty()) {
            addInitialData(initialData);
        }
    }

    /* ---------- initial bulk load ---------- */
    private void addInitialData(List<double[]> data) {
        EvenLSH lsh = new EvenLSH(data.get(0).length, /*dummy*/10);
        lsh.updateCriticalValues(data);

        for (double[] vec : data) {
            try {
                int bucketCode = lsh.getBucketId(vec);
                add(UUID.randomUUID().toString(), vec, bucketCode, false, data);
            } catch (Exception ex) {
                logger.error("Error adding vector: {}", ex.getMessage(), ex);
            }
        }
    }

    /* ---------- public API ---------- */
    public int add(String id,
                   double[] vector,
                   int bucketCode,
                   boolean useFakePoints,
                   List<double[]> baseVectors) throws Exception {

        byte[] encrypted = EncryptionUtils.encryptVector(vector, currentKey);
        int    index     = baseVectors.indexOf(vector);
        if (index < 0)
            throw new IllegalArgumentException("Vector not in baseVectors");

        EncryptedPoint ep = new EncryptedPoint(encrypted,
                "bucket_v" + bucketCode,
                id, index);
        encryptedPoints.put(id, ep);

        bucketToShard.putIfAbsent(bucketCode, bucketCode % NUM_SHARDS);

        int fakeAdded = 0;
        for (int t = 0; t < numHashTables; t++) {
            hashTables.get(t)
                    .computeIfAbsent(bucketCode, k -> new ArrayList<>())
                    .add(ep);
            if (useFakePoints) fakeAdded++;
        }
        return fakeAdded;
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
            @SuppressWarnings("unchecked")
            // Load the encrypted points and hash tables from files
            Map<String,EncryptedPoint> epMap = PersistenceUtils.loadObject(directoryPath+"/encrypted_points.ser", Map.class);            List<Map<Integer, List<EncryptedPoint>>> htList = PersistenceUtils.loadObject(directoryPath + "/hash_tables.ser", List.class);
            this.encryptedPoints.putAll(epMap);
            this.hashTables.addAll(htList);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();

        }
    }

    // Remove a point by ID from the index
    public void remove(String id) {
        encryptedPoints.remove(id);

        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            table.forEach((bucketId, points) -> points.removeIf(p -> p.getPointId().equals(id)));
        }
    }

    public void clear() {
        // Clear the encrypted points map
        encryptedPoints.clear();

        // Iterate over each hash table and clear the entries
        for (Map<Integer, List<EncryptedPoint>> table : hashTables) {
            table.clear();
        }

        // Optionally, log that the index has been cleared for debugging or tracking
        System.out.println("SecureLSHIndex has been cleared.");
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


    /**
     * Re-encrypt all buckets that belong to the given shard.
     * Called by the background ReEncryptor or unit-tests.
     *
     * @param shardId the shard whose key has just rotated
     * @param km      the KeyManager that holds old & new shard keys
     */
    public void reEncryptShard(int shardId, KeyManager km) throws Exception {
        if (SystemConfig.PROFILER_ENABLED) profiler.start("reenc-batch");

        // fetch keys: latest + previous version for this shard
        int   latestVer = km.getTimeVersion();                 // global counter
        SecretKey newKey = km.getShardKey(shardId, latestVer);
        SecretKey oldKey = km.getShardKey(shardId, latestVer - 1);

        if (newKey == null || oldKey == null) {
            logger.warn("Shard {} reEncrypt: missing keys (old={}, new={})",
                    shardId, oldKey, newKey);
            return;
        }

        /* iterate through every hash-table & bucket */
        for (Map<Integer,List<EncryptedPoint>> table : hashTables) {
            for (Map.Entry<Integer,List<EncryptedPoint>> e : table.entrySet()) {

                int bucketCode = e.getKey();
                if (bucketToShard.getOrDefault(bucketCode, -1) != shardId) continue;

                for (EncryptedPoint ep : e.getValue()) {
                    byte[] reEnc = EncryptionUtils.reEncryptData(
                            ep.getCiphertext(), oldKey, newKey);
                    ep.setCiphertext(reEnc);
                }
            }
        }
        if (SystemConfig.PROFILER_ENABLED) profiler.stop("reenc-batch");

        logger.info("Shard {} re-encrypted with version v{}", shardId, latestVer);
    }

    public EncryptedPoint findById(String id) {
        return encryptedPoints.get(id);  // Retrieve the EncryptedPoint by its ID
    }


}
