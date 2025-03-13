package com.fspann;

import com.fspann.encryption.EncryptionUtils;
import com.fspann.index.EvenLSH;
import com.fspann.index.SecureLSHIndex;
import com.fspann.keymanagement.KeyManager;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryClientHandler;
import com.fspann.query.QueryGenerator;
import com.fspann.query.QueryProcessor;
import com.fspann.query.QueryToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);

    private final SecureLSHIndex index;
    private final KeyManager keyManager;
    private final QueryClientHandler queryClientHandler;
    private final QueryGenerator queryGenerator;
    private final QueryProcessor queryProcessor;
    private final ConcurrentHashMap<String, EncryptedPoint> encryptedDataStore;
    private final ConcurrentHashMap<String, String> metadata;
    private final ExecutorService queryExecutor;
    private int operationCount;

    public ForwardSecureANNSystem(int dimensions, int numHashTables, int numIntervals, int maxBucketSize, int targetBucketSize, List<double[]> initialData) {
        this.keyManager = new KeyManager(1000);
        EvenLSH initialLsh = new EvenLSH(dimensions, numIntervals, initialData != null ? initialData : Collections.emptyList());
        this.index = new SecureLSHIndex(dimensions, numHashTables, numIntervals, null, maxBucketSize, targetBucketSize, initialData);
        this.queryGenerator = new QueryGenerator(initialLsh, keyManager);
        this.queryClientHandler = new QueryClientHandler(keyManager);
        this.queryProcessor = new QueryProcessor(new HashMap<>(), keyManager);
        this.encryptedDataStore = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
        this.queryExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.operationCount = 0;
        initializeKeys();
    }

    private void initializeKeys() {
        try {
            keyManager.generateMasterKey();
            index.setCurrentKey(keyManager.getCurrentKey());
        } catch (Exception e) {
            logger.error("Failed to initialize keys", e);
            throw new RuntimeException("Failed to initialize keys: " + e.getMessage());
        }
    }

    // Rest of the class remains unchanged...

    public static void main(String[] args) {
        try {
            logger.info("Starting ForwardSecureANNSystem...");
            int dimensions = 128;
            int numHashTables = 5;
            int numIntervals = 15;
            int maxBucketSize = 1000;
            int targetBucketSize = 1500;

            List<double[]> initialData = Collections.emptyList();
            ForwardSecureANNSystem system = new ForwardSecureANNSystem(dimensions, numHashTables, numIntervals, maxBucketSize, targetBucketSize, initialData);

            double[] vector1 = new double[dimensions];
            Arrays.fill(vector1, 1.0);
            double[] vector2 = new double[dimensions];
            Arrays.fill(vector2, 2.0);
            system.insert("point1", vector1);
            system.insert("point2", vector2);

            double[] queryVector = new double[dimensions];
            Arrays.fill(queryVector, 1.5);
            List<double[]> nearestNeighbors = system.query(queryVector, 1);
            logger.info("Nearest neighbor: {}", Arrays.toString(nearestNeighbors.get(0)));

            system.shutdown();
            logger.info("ForwardSecureANNSystem shutdown successfully");
        } catch (Exception e) {
            logger.error("Error executing ForwardSecureANNSystem", e);
        }
    }
}