package com.fspann.query.core;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;

public final class QueryTokenFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final PartitionedIndexService partition;
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

        log.info("QueryTokenFactory (MSANNP): divisions={}", divisions);
    }

    /**
     * Build MSANNP QueryToken.
     */
    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec);
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        int dim = vec.length;

        // 1) Compute codes for partition lookup
        BitSet[] codes = partition.code(vec);

        // 2) Encrypt query (for forward secrecy)
        KeyVersion kv = keyService.getCurrentVersion();
        SecretKey sk = kv.getKey();
        EncryptedPoint enc = crypto.encryptToPoint("query", vec, sk);

        // 3) Build token
        return new QueryToken(
                Collections.emptyList(),   // no LSH buckets
                codes,                     // MSANNP codes
                enc.getIv(),
                enc.getCiphertext(),
                topK,
                divisions,
                "dim_" + dim + "_v" + kv.getVersion(),
                dim,
                kv.getVersion()
        );
    }

    public QueryToken derive(QueryToken tok, int newTopK) {
        return new QueryToken(
                tok.getTableBuckets(),
                tok.getCodes(),
                tok.getIv(),
                tok.getEncryptedQuery(),
                newTopK,
                tok.getNumTables(),
                tok.getEncryptionContext(),
                tok.getDimension(),
                tok.getVersion()
        );
    }
}
