package com.fspann.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fspann.config.SystemConfig;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryToken;
import com.fspann.utils.PersistenceUtils;
import com.fspann.utils.Profiler;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe, secure LSH index over encrypted points.
 */
public class SecureLSHIndex {

    /* ---------- constants ---------- */
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);
    private static final int NUM_SHARDS = 32;      // constant

    /* ---------- fields ---------- */
    private final int numHashTables;
    private final int numShards;
    private SecretKey currentKey;
    private final Profiler profiler;
    private final ReadWriteLock indexLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<String, EncryptedPoint> encryptedPoints = new ConcurrentHashMap<>();
    private final List<ConcurrentHashMap<Integer, CopyOnWriteArrayList<EncryptedPoint>>> hashTables = new ArrayList<>();
    private final ConcurrentHashMap<Integer, Integer> bucketToShard = new ConcurrentHashMap<>();
    private Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> index = new ConcurrentHashMap<>();

    /* ---------- ctor ---------- */
    public SecureLSHIndex(int numHashTables,
                          int numShards,
                          SecretKey key,
                          List<double[]> initialData) {
        this.numHashTables = numHashTables;
        this.numShards = numShards;
        this.currentKey = key;
        this.profiler = new Profiler();

        for (int i = 0; i < numHashTables; i++) {
            hashTables.add(new ConcurrentHashMap<>());
        }

        if (initialData != null && !initialData.isEmpty()) {
            addInitialData(initialData);
        }
    }

    public void addEncryptedPoint(Integer key, EncryptedPoint point) {
        index.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(point);
    }

    /* ---------- helper ---------- */
    private int findVectorIndex(double[] vector, List<double[]> baseVectors) {
        for (int i = 0; i < baseVectors.size(); i++) {
            if (Arrays.equals(vector, baseVectors.get(i))) {
                return i;
            }
        }
        return -1;
    }

    /* ---------- initial bulk load ---------- */
    private void addInitialData(List<double[]> data) {
        EvenLSH lsh = new EvenLSH(data.get(0).length, 10);
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
        indexLock.writeLock().lock();
        try {
            int index = findVectorIndex(vector, baseVectors);
            if (index < 0) {
                throw new IllegalArgumentException("Vector not in baseVectors");
            }

            byte[] iv = EncryptionUtils.generateIV();
            byte[] encrypted = EncryptionUtils.encryptVector(vector, iv, currentKey);
            EncryptedPoint ep = new EncryptedPoint(
                    encrypted,
                    String.valueOf(bucketCode),  // bucketId
                    id,                          // pointId
                    index,                       // index in baseVectors
                    iv,
                    id                           // id field
            );

            encryptedPoints.put(id, ep);
            bucketToShard.putIfAbsent(bucketCode, bucketCode % numShards);

            int fakeAdded = 0;
            for (int t = 0; t < numHashTables; t++) {
                hashTables.get(t)
                        .computeIfAbsent(bucketCode, k -> new CopyOnWriteArrayList<>())
                        .add(ep);
                if (useFakePoints) {
                    fakeAdded++;
                }
            }
            return fakeAdded;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    // Save the encrypted index to disk
    public void saveIndex(String directoryPath) {
        indexLock.writeLock().lock();
        try {
            PersistenceUtils.saveObject(encryptedPoints, directoryPath + "/encrypted_points.ser");
            PersistenceUtils.saveObject(hashTables,   directoryPath + "/hash_tables.ser");
            logger.info("Index saved to: {}", directoryPath);
        } catch (IOException e) {
            logger.error("Failed to save index to: {}", directoryPath, e);
            throw new RuntimeException("Failed to save index", e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    // Load the encrypted index from disk
    @SuppressWarnings("unchecked")
    public void loadIndex(String directoryPath) {
        indexLock.writeLock().lock();
        try {
            encryptedPoints.clear();
            hashTables.forEach(Map::clear);

            // Load encrypted points
            Map<String, EncryptedPoint> epMap = PersistenceUtils.loadObject(
                    directoryPath + "/encrypted_points.ser",
                    new TypeReference<ConcurrentHashMap<String, EncryptedPoint>>() {}
            );

            // Load hash tables with proper type conversion
            List<ConcurrentHashMap<Integer, List<EncryptedPoint>>> htList = PersistenceUtils.loadObject(
                    directoryPath + "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\index_backup\\hash_tables.ser", new TypeReference<List<ConcurrentHashMap<Integer, List<EncryptedPoint>>>>() {}
            );

            encryptedPoints.putAll(epMap);

            // Convert loaded List<EncryptedPoint> to CopyOnWriteArrayList
            for (int i = 0; i < Math.min(hashTables.size(), htList.size()); i++) {
                ConcurrentHashMap<Integer, CopyOnWriteArrayList<EncryptedPoint>> targetTable = hashTables.get(i);
                htList.get(i).forEach((bucketId, points) ->
                        targetTable.put(bucketId, new CopyOnWriteArrayList<>(points))
                );
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to load index from {}", directoryPath, e);
            throw new RuntimeException("Failed to load index", e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    // Remove a point by ID from the index
    public void remove(String id) {
        indexLock.writeLock().lock();
        try {
            encryptedPoints.remove(id);
            for (ConcurrentHashMap<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : hashTables) {
                table.forEach((bucketId, points) -> points.removeIf(p -> p.getPointId().equals(id)));
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    // Clear the entire index
    public void clear() {
        indexLock.writeLock().lock();
        try {
            encryptedPoints.clear();
            for (Map<Integer, ?> table : hashTables) {
                table.clear();
            }
            logger.info("SecureLSHIndex has been cleared.");
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    // Set the current key (for rehashing)
    public void setCurrentKey(SecretKey key) {
        this.currentKey = key;
    }

    // Find nearest neighbors using the encrypted query and LSH
    public List<EncryptedPoint> findNearestNeighborsEncrypted(QueryToken queryToken) {
        indexLock.readLock().lock();
        try {
            if (queryToken == null) {
                throw new IllegalArgumentException("QueryToken cannot be null");
            }
            Set<EncryptedPoint> candidates = new HashSet<>();
            List<Integer> candidateBuckets = queryToken.getCandidateBuckets();
            int numTables = Math.min(queryToken.getNumTables(), numHashTables);
            if (candidateBuckets == null || candidateBuckets.isEmpty()) {
                return Collections.emptyList();
            }
            for (int i = 0; i < numTables; i++) {
                for (Integer bucketId : candidateBuckets) {
                    CopyOnWriteArrayList<EncryptedPoint> bucket = hashTables.get(i)
                            .getOrDefault(bucketId, new CopyOnWriteArrayList<>());
                    candidates.addAll(bucket);
                }
            }
            List<EncryptedPoint> result = new ArrayList<>(candidates);
            return result.subList(0, Math.min(queryToken.getTopK(), result.size()));
        } finally {
            indexLock.readLock().unlock();
        }
    }

    // Rehash the index with a new key
    public void rehash(KeyManager keyManager, String context) throws Exception {
        indexLock.writeLock().lock();
        try {
            SecretKey oldKey = keyManager.getPreviousKey();
            SecretKey newKey = keyManager.getCurrentKey();
            if (newKey == null) {
                throw new IllegalStateException("No current key available for rehashing");
            }
            logger.info("Rehashing with context: {}", context);
            for (ConcurrentHashMap<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : hashTables) {
                for (CopyOnWriteArrayList<EncryptedPoint> bucket : table.values()) {
                    for (EncryptedPoint point : bucket) {
                        Pair<byte[], byte[]> reEnc = EncryptionUtils.reEncryptData(
                                point.getCiphertext(), point.getIV(), oldKey, newKey);
                        point.setCiphertext(reEnc.getLeft());
                    }
                }
            }
            this.currentKey = newKey;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void reEncryptShard(int shardId, KeyManager km, List<SecretKey> previousKeys) {
        // Retrieve the shard from the index using the shardId
        CopyOnWriteArrayList<EncryptedPoint> shard = index.get(shardId);

        // Check if the shard is valid (not null)
        if (shard != null) {
            for (EncryptedPoint ep : shard) {
                try {
                    // Call reEncrypt with both the KeyManager and the previous keys
                    ep.reEncrypt(km, previousKeys);
                } catch (Exception e) {
                    logger.error("Failed to re-encrypt EncryptedPoint for shard {}: {}", shardId, ep.getPointId(), e);
                }
            }
        }
    }

    // Find by ID
    public EncryptedPoint findById(String id) {
        indexLock.readLock().lock();
        try {
            return encryptedPoints.get(id);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * @return the number of hash tables used by this index
     */
    public int getNumHashTables() {
        return numHashTables;
    }
}
