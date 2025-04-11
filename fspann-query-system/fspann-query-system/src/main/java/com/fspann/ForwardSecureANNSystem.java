package java.com.fspann;

import java.util.concurrent.atomic.AtomicInteger;
import java.com.fspann.data.DataLoader;
import java.com.fspann.index.EvenLSH;
import java.com.fspann.keymanagement.KeyManager;
import java.com.fspann.keymanagement.KeyVersionManager;
import java.com.fspann.encryption.EncryptionUtils;
import java.com.fspann.index.SecureLSHIndex;
import java.com.fspann.query.EncryptedPoint;
import java.com.fspann.query.QueryGenerator;
import java.com.fspann.query.QueryProcessor;
import java.com.fspann.query.QueryToken;
import java.com.fspann.utils.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.*;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final SecureLSHIndex index;
    private final KeyVersionManager keyVersionManager;
    private final QueryProcessor queryProcessor;
    private final LRUCache<QueryToken, List<EncryptedPoint>> queryCache;
    private final Map<String, EncryptedPoint> encryptedDataStore;
    private int totalInsertTimeMs = 0;
    private int totalFakePoints = 0;
    private int totalRehashes = 0;

    private final AtomicInteger operationCount;

    public ForwardSecureANNSystem(String basePath, String queryPath, String groundTruthPath,
                                  int numHashTables, int numIntervals,
                                  int maxBucketSize, int targetBucketSize) throws IOException {
        // Initialize KeyManager and KeyVersionManager
        KeyManager keyManager = new KeyManager(1000);  // Rotation interval
        this.keyVersionManager = new KeyVersionManager(keyManager, 1000);  // KeyVersionManager with KeyManager

        this.queryCache = new LRUCache<>(100);  // LRUCache for query results
        this.encryptedDataStore = new HashMap<>();
        Map<String, String> metadata = new HashMap<>();
        this.operationCount = new AtomicInteger(0);

        // Load the datasets
        logger.info("[STEP] 📥 Loading Datasets...");
        DataLoader dataLoader = new DataLoader();
        List<double[]> baseVectors = dataLoader.readFvecs(basePath);
        List<double[]> queryVectors = dataLoader.readFvecs(queryPath);
        dataLoader.readIvecs(groundTruthPath); // not used but keep it for now

        logger.info("[STEP] ✅ Dataset loading complete.");

        // Initialize LSH and the index
        EvenLSH initialLsh = new EvenLSH(baseVectors.getFirst().length, numIntervals, baseVectors);
        this.index = new SecureLSHIndex(numHashTables, keyManager.getCurrentKey(), baseVectors);
        QueryGenerator queryGenerator = new QueryGenerator(initialLsh, keyManager);  // Fixed: KeyManager provided
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager);  // Fixed: KeyManager provided
    }

    // Insert a new vector and handle key rotation and encryption
    public void insert(String id, double[] vector) throws Exception {
        operationCount.incrementAndGet();

        // Get the current versioned key
        SecretKey currentKey = keyVersionManager.getCurrentKey();

        // Encrypt the vector
        byte[] encryptedVector = EncryptionUtils.encryptVector(vector, currentKey);
        EncryptedPoint encryptedPoint = new EncryptedPoint(encryptedVector, "bucket_v" + currentKey.hashCode(), id);
        encryptedDataStore.put(id, encryptedPoint);

        // Add fake points if necessary (not used currently but could be in future)
        int addedFakes = index.add(id, vector, true);

        // Check if rehashing is required based on the number of operations
        if (keyVersionManager.needsRotation(operationCount.get())) {
            logger.info("[STEP] 🔄 Rotating keys...");
            keyVersionManager.rotateKeys();  // Rotate keys
            index.rehash(keyVersionManager.getKeyManager(), "epoch_v" + (keyVersionManager.getTimeVersion() - 1));  // Pass KeyManager
            operationCount.set(0); // Reset operation count
        }
    }

    // Query the encrypted data store with caching mechanism
    public List<EncryptedPoint> query(QueryToken queryToken) throws Exception {
        List<EncryptedPoint> result = queryCache.get(queryToken);

        // If the result is in the cache, return it
        if (result != null) {
            logger.info("Cache hit for query: {}", queryToken);
            return result;
        }

        // Otherwise, process the query
        result = queryProcessor.processQuery(index.findNearestNeighborsEncrypted(queryToken));

        // Cache the result for future queries
        queryCache.put(queryToken, result);
        return result;
    }

    // Delete a vector from the data store and rehash if necessary
    public void delete(String id) throws Exception {
        encryptedDataStore.remove(id);
        index.remove(id);  // Remove the point from index as well
    }

    // Get the current operation count
    public int getOperationCount() {
        return operationCount.get();
    }

    // Shutdown the system and clean up resources
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem...");
    }

    public int getTotalInsertTimeMs() {
        return totalInsertTimeMs;
    }

    public void setTotalInsertTimeMs(int totalInsertTimeMs) {
        this.totalInsertTimeMs = totalInsertTimeMs;
    }

    public int getTotalFakePoints() {
        return totalFakePoints;
    }

    public void setTotalFakePoints(int totalFakePoints) {
        this.totalFakePoints = totalFakePoints;
    }

    public int getTotalRehashes() {
        return totalRehashes;
    }

    public void setTotalRehashes(int totalRehashes) {
        this.totalRehashes = totalRehashes;
    }
}
