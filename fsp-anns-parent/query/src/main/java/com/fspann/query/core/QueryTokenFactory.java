package com.fspann.query.core;

import com.fspann.common.QueryToken;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.index.paper.Coding;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * QueryTokenFactory — MSANNP ONLY
 * ===============================
 *
 * HARD GUARANTEES:
 * ----------------
 * - NO integer hashes
 * - NO tableBuckets
 * - ONLY BitSet prefix codes
 * - Query and index share identical GFunctions
 *
 * This class is now mathematically aligned with
 * Peng et al. (MSANNP, Algorithm-1 & 2).
 */
public final class QueryTokenFactory {

    private static final Logger log =
            LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final SystemConfig cfg;
    private final int tables;
    private final int divisions;

    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            SystemConfig cfg
    ) {
        this.crypto = Objects.requireNonNull(crypto);
        this.keyService = Objects.requireNonNull(keyService);
        this.cfg = Objects.requireNonNull(cfg);

        SystemConfig.PaperConfig pc = cfg.getPaper();
        this.tables = pc.getTables();
        this.divisions = pc.getDivisions();

        log.info(
                "QueryTokenFactory [MSANNP] tables={} divisions={} m={} lambda={}",
                tables, divisions, pc.getM(), pc.getLambda()
        );
    }

    // =====================================================
    // CREATE TOKEN (MSANNP)
    // =====================================================
    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec, "query vector is null");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        if (!GFunctionRegistry.isInitialized()) {
            throw new IllegalStateException(
                    "GFunctionRegistry not initialized. Build index first."
            );
        }

        SystemConfig.PaperConfig pc = cfg.getPaper();
        int dim = vec.length;

        // -----------------------------
        // Registry consistency check
        // -----------------------------
        Map<String, Object> stats = GFunctionRegistry.getStats();
        if ((int) stats.get("dimension") != dim
                || (int) stats.get("tables") != tables
                || (int) stats.get("divisions") != divisions
                || (int) stats.get("m") != pc.getM()
                || (int) stats.get("lambda") != pc.getLambda()) {
            throw new IllegalStateException(
                    "GFunctionRegistry mismatch: " + stats
            );
        }

        // =====================================================
        // MSANNP CORE: BitSet prefix codes ONLY
        // =====================================================
        BitSet[][] bitCodes = new BitSet[tables][divisions];

        for (int t = 0; t < tables; t++) {
            for (int d = 0; d < divisions; d++) {
                Coding.GFunction G =
                        GFunctionRegistry.get(dim, t, d);

                bitCodes[t][d] = Coding.C(vec, G);
            }
        }

        for (int t = 0; t < tables; t++) {
            if (bitCodes[t].length != divisions) {
                throw new IllegalStateException(
                        "BitCode dimension mismatch at table " + t
                );
            }
        }

        // =====================================================
        // Encrypt query ONCE
        // =====================================================
        KeyVersion kv = keyService.getCurrentVersion();
        byte[] iv = EncryptionUtils.generateIV();
        byte[] ct = crypto.encryptQuery(vec, kv.getKey(), iv);

        return new QueryToken(
                bitCodes,
                iv,
                ct,
                topK,
                tables,
                dim,
                kv.getVersion(),
                pc.getLambda(),
                "dim_" + dim + "_v" + kv.getVersion()
        );
    }

    // =====================================================
    // DERIVE TOKEN (change topK only)
    // =====================================================
    public QueryToken derive(QueryToken tok, int newTopK) {
        Objects.requireNonNull(tok, "token is null");
        if (newTopK <= 0)
            throw new IllegalArgumentException("newTopK must be > 0");

        return new QueryToken(
                tok.getBitCodes(),
                tok.getIv(),
                tok.getEncryptedQuery(),
                newTopK,
                tok.getNumTables(),
                tok.getDimension(),
                tok.getVersion(),
                tok.getLambda(),
                tok.getEncryptionContext()
        );
    }

    // =====================================================
    // DIAGNOSTICS
    // =====================================================
    public Map<String, Object> getDiagnostics() {
        Map<String, Object> diag = new LinkedHashMap<>();
        diag.put("tables", tables);
        diag.put("divisions", divisions);
        diag.put("m", cfg.getPaper().getM());
        diag.put("lambda", cfg.getPaper().getLambda());
        diag.put("registryInitialized", GFunctionRegistry.isInitialized());
        if (GFunctionRegistry.isInitialized()) {
            diag.put("registryStats", GFunctionRegistry.getStats());
        }
        return diag;
    }
}
