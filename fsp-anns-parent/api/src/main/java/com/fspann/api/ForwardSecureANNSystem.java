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
import com.fspann.common.Profiler;
import com.fspann.common.LRUCache;
import com.fspann.loader.DefaultDataLoader;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.UUID;

/**
 * Facade for the Forward-Secure ANN system.
 */
public class ForwardSecureANNSystem {
    private final KeyManager keyManager;
    private final KeyLifeCycleService keyService;
    private final CryptoService cryptoService;
    private final SecureLSHIndexService indexService;
    private final QueryTokenFactory tokenFactory;
    private final QueryService queryService;
    private final Profiler profiler;
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

        // Load configuration
        ApiSystemConfig apiConfig = new ApiSystemConfig(configPath);
        SystemConfig cfg = apiConfig.getConfig();

        // Setup key management using explicit keysFilePath
        KeyRotationPolicy policy = new KeyRotationPolicy(
                (int) cfg.getOpsThreshold(),
                cfg.getAgeThresholdMs()
        );
        this.keyManager = new KeyManager(keysFilePath);
        this.keyService = new KeyRotationServiceImpl(keyManager, policy);

        // Crypto service (stateless)
        this.cryptoService = new AesGcmCryptoService();

        // Build core index and LSH helper
        int numShards = cfg.getNumShards();
        SecureLSHIndex coreIndex = new SecureLSHIndex(1, numShards);
        EvenLSH lshHelper = new EvenLSH(dimensions, numShards);
        this.indexService = new SecureLSHIndexService(
                coreIndex,
                cryptoService,
                keyService,
                lshHelper
        );

        // Load and insert base data
        DefaultDataLoader loader = new DefaultDataLoader();
        List<double[]> vectors = loader.loadData(dataPath, dimensions);
        for (double[] vec : vectors) {
            insert(UUID.randomUUID().toString(), vec);
        }

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

        // Utilities
        this.profiler = new Profiler();
        this.cache    = new LRUCache<>(1000);
    }

    /** Insert a new vector into the index */
    public void insert(String id, double[] vector) {
        profiler.start("insert");
        indexService.insert(id, vector);
        profiler.stop("insert");
    }

    /** Query the top-K nearest neighbors */
    public List<QueryResult> query(double[] vector, int k) throws Exception {
        QueryToken token = tokenFactory.create(vector, k);
        List<QueryResult> cached = cache.get(token);
        if (cached != null) {
            return cached;
        }
        List<QueryResult> results = queryService.search(token);
        cache.put(token, results);
        return results;
    }

    /** Clean up resources, persist state as needed. */
    public void shutdown() {
        // optionally save index, keys, metadata
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

        // Example usage
        List<QueryResult> res = sys.query(new double[dimensions], 10);
        sys.shutdown();
    }
}
