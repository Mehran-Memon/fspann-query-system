package com.fspann.api;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import com.fspann.index.core.SecureLSHIndex;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryService;
import com.fspann.query.service.QueryServiceImpl;
import com.fspann.common.LRUCache;
import com.fspann.loader.DefaultDataLoader;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.UUID;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

/**
 * Facade for the Forward-Secure ANN system with live metrics.
 */
public class ForwardSecureANNSystem {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystem.class);
    private final PrometheusMeterRegistry registry;

    private final KeyManager keyManager;
    private final KeyLifeCycleService keyService;
    private final CryptoService cryptoService;
    private final SecureLSHIndexService indexService;
    private final QueryTokenFactory tokenFactory;
    private final QueryService queryService;
    private final LRUCache<QueryToken, List<QueryResult>> cache;

    /**
     * @param configPath    Path to JSON/YAML config file
     * @param dataPath      Path to feature data file
     * @param keysFilePath  Path to serialized keys file for KeyManager
     * @param dimensions    Dimensionality of vectors
     */
    public ForwardSecureANNSystem(
            String configPath,
            String dataPath,
            String keysFilePath,
            int dimensions
    ) throws Exception {
        logger.info("Initializing ForwardSecureANNSystem with config={}, data={}, keys={}, dims={}",
                configPath, dataPath, keysFilePath, dimensions);

        // 1) start Prometheus pull server
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new HTTPServer(9090);

        // Load configuration
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        SystemConfig cfg = apiConfig.getConfig();
        logger.info("Loaded system configuration");

        // Setup key management
        KeyRotationPolicy policy = new KeyRotationPolicy(
                (int) cfg.getOpsThreshold(),
                cfg.getAgeThresholdMs()
        );
        this.keyManager = new KeyManager(keysFilePath);
        this.keyService = new KeyRotationServiceImpl(keyManager, policy);
        logger.info("KeyManager initialized, using keys file at {}", keysFilePath);

        // Crypto service
        this.cryptoService = new AesGcmCryptoService();
        logger.info("CryptoService (AES-GCM) ready");

        // Build core index
        int numShards = cfg.getNumShards();
        SecureLSHIndex coreIndex = new SecureLSHIndex(1, numShards);
        EvenLSH lshHelper = new EvenLSH(dimensions, numShards);
        this.indexService = new SecureLSHIndexService(
                coreIndex,
                cryptoService,
                keyService,
                lshHelper
        );
        logger.info("SecureLSHIndexService initialized with {} shards", numShards);

        // Cache
        this.cache    = new LRUCache<>(1000);
        logger.info("Cache initialized");

        // Load and insert base data
        logger.info("Loading base data from {}", dataPath);
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dimensions);
        logger.info("Loaded {} vectors for initial indexing", vectors.size());
        for (double[] vec : vectors) {
            insert(UUID.randomUUID().toString(), vec);
        }
        logger.info("Initial data insertion complete");

        // Setup query pipeline
        EvenLSH queryLsh = new EvenLSH(dimensions, numShards);
        this.tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                queryLsh,
                /* expansionRange= */ 1,
                /* numTables=     */ 1
        );
        this.queryService = new QueryServiceImpl(indexService, cryptoService, keyService);
        logger.info("QueryService initialized and ready");
    }

    /** Insert a new vector into the index */
    public void insert(String id, double[] vector) {
        logger.debug("Inserting vector id={} into index", id);
        Timer.Sample sample = Timer.start(registry);
        indexService.insert(id, vector);
        sample.stop(registry.timer("fspann_insert_time"));
        registry.counter("fspann_insert_count").increment();
        logger.debug("Insert complete for id={}", id);
    }

    /** Query the top-K nearest neighbors */
    public List<QueryResult> query(double[] vector, int k) throws Exception {
        logger.info("Executing query topK={} for vector length {}", k, vector.length);
        Timer.Sample sample = Timer.start(registry);
        QueryToken token = tokenFactory.create(vector, k);
        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            logger.debug("Cache hit for token={}", token);
            sample.stop(registry.timer("fspann_query_time"));
            return cached;
        }
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        sample.stop(registry.timer("fspann_query_time"));
        logger.info("Query returned {} results", results.size());
        return results;
    }

    /** Clean up resources, persist state as needed. */
    public void shutdown() {
        logger.info("Shutting down ForwardSecureANNSystem");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: <configPath> <dataPath> <keysFilePath> <dimensions>");
            System.exit(1);
        }
        String configFile  = args[0];
        String dataPath    = args[1];
        String keysFile    = args[2];
        int dimensions     = Integer.parseInt(args[3]);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile,
                dataPath,
                keysFile,
                dimensions
        );
        sys.query(new double[dimensions], 10);
        sys.shutdown();
    }
}
