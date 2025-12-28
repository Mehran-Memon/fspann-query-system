package com.fspann.query.core;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final SystemConfig cfg;
    private final int divisions;
    private final int tables;


    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            PartitionedIndexService partition,
            SystemConfig cfg,
            int divisions,
            int tables
    ) {
        this.crypto = Objects.requireNonNull(crypto);
        this.keyService = Objects.requireNonNull(keyService);
        this.cfg = Objects.requireNonNull(cfg);
        this.divisions = divisions;
        this.tables = tables;

        log.info(
                "QueryTokenFactory (MSANNP v2.1): divisions={} tables={}",
                divisions, tables
        );

        SystemConfig.PaperConfig pc = cfg.getPaper();
        log.info(
                "CONFIG ASSERT: m={} lambda={} tables={} divisions={} seed={}",
                pc.getM(), pc.getLambda(), pc.getTables(), pc.getDivisions(), pc.getSeed()
        );

        if (tables != pc.getTables()) {
            throw new IllegalStateException(
                    "TokenFactory tables mismatch: ctor=" + tables +
                            " config=" + pc.getTables()
            );
        }
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

        if (!GFunctionRegistry.isInitialized()) {
            throw new IllegalStateException(
                    "GFunctionRegistry not initialized. Build index first."
            );
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int L = pc.getTables();
        int j = pc.getDivisions();
        int dim = vec.length;

        // Registry sanity
        Map<String, Object> stats = GFunctionRegistry.getStats();
        if ((int) stats.get("dimension") != dim
                || (int) stats.get("tables") != L
                || (int) stats.get("divisions") != j
                || (int) stats.get("m") != pc.getM()
                || (int) stats.get("lambda") != pc.getLambda()) {
            throw new IllegalStateException(
                    "GFunctionRegistry mismatch: " + stats
            );
        }

        // === MSANNP v2: integer hashes only ===
        int[][] hashesByTable = GFunctionRegistry.hashAllTables(vec);

        if (hashesByTable == null || hashesByTable.length != L) {
            throw new IllegalStateException(
                    "Invalid hash tables returned: expected " + L
            );
        }

        for (int t = 0; t < L; t++) {
            if (hashesByTable[t] == null || hashesByTable[t].length != j) {
                throw new IllegalStateException(
                        "Invalid hash row at table " + t +
                                " expected " + j + " divisions"
                );
            }
        }

        // Encrypt query ONCE
        KeyVersion kv = keyService.getCurrentVersion();
        byte[] iv = EncryptionUtils.generateIV();
        byte[] ct = crypto.encryptQuery(vec, kv.getKey(), iv);

        return new QueryToken(
                Collections.emptyList(),   // legacy tableBuckets (unused)
                hashesByTable,
                iv,
                ct,
                topK,
                L,
                "dim_" + dim + "_v" + kv.getVersion(),
                dim,
                kv.getVersion(),
                pc.getLambda()
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
                tok.getHashesByTable(),
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
        diag.put("m", cfg.getPaper().getM());
        diag.put("lambda", cfg.getPaper().getLambda());
        diag.put("baseSeed", cfg.getPaper().getSeed());
        diag.put("registryInitialized", GFunctionRegistry.isInitialized());
        if (GFunctionRegistry.isInitialized()) {
            diag.put("registryStats", GFunctionRegistry.getStats());
        }
        return diag;
    }
}