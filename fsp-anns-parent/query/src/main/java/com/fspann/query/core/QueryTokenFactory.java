package com.fspann.query.core;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * QueryTokenFactory - MSANNP Query Token Generation
 * ==================================================
 *
 * KEY CHANGE (v2.0):
 * ------------------
 * Now uses GFunctionRegistry directly for code generation.
 * This guarantees that query codes use the EXACT SAME GFunction
 * parameters (alpha, r, omega) as the index.
 *
 * PREVIOUS BUG:
 * -------------
 * Called partition.code(vec, seedT) which internally created NEW GFunctions
 * with hardcoded omega=1.0, causing index/query mismatch and ~0 candidates.
 *
 * FIX:
 * ----
 * Uses GFunctionRegistry.codeForTable() which returns codes using the
 * cached, data-adaptive GFunctions initialized during indexing.
 *
 * CONSISTENCY GUARANTEE:
 * ----------------------
 * Index: insert() -> codeForTable() -> GFunctionRegistry.codeForTable()
 * Query: create() -> GFunctionRegistry.codeForTable()
 *
 * Both paths use the SAME cached GFunction objects.
 */
public final class QueryTokenFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final PartitionedIndexService partition;  // kept for backward compatibility
    private final SystemConfig cfg;
    private final int divisions;

    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            PartitionedIndexService partition,
            SystemConfig cfg,
            int divisions
    ) {
        this.crypto = Objects.requireNonNull(crypto);
        this.keyService = Objects.requireNonNull(keyService);
        this.partition = Objects.requireNonNull(partition);
        this.cfg = Objects.requireNonNull(cfg);
        this.divisions = divisions;

        log.info("QueryTokenFactory (MSANNP v2.0): divisions={}, using GFunctionRegistry", divisions);
        SystemConfig.PaperConfig pc = cfg.getPaper();
        log.info(
                "CONFIG ASSERT: m={} lambda={} tables={} divisions={} seed={}",
                pc.m, pc.lambda, pc.getTables(), pc.divisions, pc.seed
        );

    }

    /**
     * Build MSANNP QueryToken.
     *
     * Uses GFunctionRegistry directly for code generation to ensure
     * perfect consistency with index codes.
     */
    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec, "Query vector cannot be null");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        int dim = vec.length;

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int L = pc.getTables();              // number of tables
        int j = pc.divisions;                // divisions per table

        // Validate GFunctionRegistry is initialized
        if (!GFunctionRegistry.isInitialized()) {
            throw new IllegalStateException(
                    "GFunctionRegistry not initialized! " +
                            "Index must be built (vectors inserted) before querying."
            );
        }

        Map<String, Object> stats = GFunctionRegistry.getStats();
        if (!stats.get("tables").equals(pc.getTables())
                || !stats.get("divisions").equals(pc.divisions)
                || !stats.get("m").equals(pc.m)
                || !stats.get("lambda").equals(pc.lambda)) {
            throw new IllegalStateException(
                    "GFunctionRegistry configuration mismatch: " + stats
            );
        }

        // Validate vector
        GFunctionRegistry.validateVector(vec);

        // ============================================================
        // KEY FIX: Use GFunctionRegistry directly for code generation
        // This ensures query codes match index codes exactly
        // ============================================================
        BitSet[][] codesByTable = GFunctionRegistry.codeAllTables(vec);
        // Validate codes
        if (codesByTable == null || codesByTable.length != L) {
            throw new IllegalStateException(
                    "GFunctionRegistry returned invalid codes: expected " + L +
                            " tables, got " + (codesByTable == null ? "null" : codesByTable.length)
            );
        }
        if (codesByTable != null && codesByTable.length > 0 && codesByTable[0].length > 0) {
            StringBuilder sb = new StringBuilder();
            for (int b = 0; b < Math.min(32, codesByTable[0][0].length()); b++) {
                sb.append(codesByTable[0][0].get(b) ? '1' : '0');
            }
            log.info("QUERY CODE table=0 div=0 first32bits: {}", sb.toString());
        }

        for (int t = 0; t < L; t++) {
            if (codesByTable[t] == null || codesByTable[t].length != j) {
                throw new IllegalStateException(
                        "Invalid codes for table " + t + ": expected " + j +
                                " divisions, got " + (codesByTable[t] == null ? "null" : codesByTable[t].length)
                );
            }
        }

        // Encrypt query
        KeyVersion kv = keyService.getCurrentVersion();
        byte[] iv = EncryptionUtils.generateIV();
        byte[] ct = crypto.encryptQuery(vec, kv.getKey(), iv);

        int lambda = pc.lambda;

        log.debug("QueryToken created: dim={} K={} λ={} tables={} divisions={}",
                dim, topK, lambda, L, j);

        return new QueryToken(
                Collections.emptyList(),  // legacy tableBuckets (unused in MSANNP)
                codesByTable,
                iv,
                ct,
                topK,
                L,
                "dim_" + dim + "_v" + kv.getVersion(),
                dim,
                kv.getVersion(),
                lambda
        );
    }

    /**
     * Derive a new token with different topK from an existing token.
     * Reuses the same codes (no recomputation needed).
     */
    public QueryToken derive(QueryToken tok, int newTopK) {
        Objects.requireNonNull(tok, "Token cannot be null");
        if (newTopK <= 0) throw new IllegalArgumentException("newTopK must be > 0");

        return new QueryToken(
                tok.getTableBuckets(),
                tok.getCodesByTable(),
                tok.getIv(),
                tok.getEncryptedQuery(),
                newTopK,
                tok.getNumTables(),
                tok.getEncryptionContext(),
                tok.getDimension(),
                tok.getVersion(),
                tok.getLambda()
        );
    }

    /**
     * Get diagnostic information about the factory configuration.
     */
    public Map<String, Object> getDiagnostics() {
        Map<String, Object> diag = new LinkedHashMap<>();
        diag.put("divisions", divisions);
        diag.put("tables", cfg.getPaper().getTables());
        diag.put("m", cfg.getPaper().m);
        diag.put("lambda", cfg.getPaper().lambda);
        diag.put("baseSeed", cfg.getPaper().seed);
        diag.put("registryInitialized", GFunctionRegistry.isInitialized());
        if (GFunctionRegistry.isInitialized()) {
            diag.put("registryStats", GFunctionRegistry.getStats());
        }
        return diag;
    }
}