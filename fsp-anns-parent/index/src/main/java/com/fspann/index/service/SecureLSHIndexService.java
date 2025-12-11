package com.fspann.index.service;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.EvenLSH;
import com.fspann.index.paper.PartitionedIndexService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SecureLSHIndexService – Option-C (Unified SANNP/mSANNP Architecture)
 * --------------------------------------------------------------------
 * Only one engine exists: PartitionedIndexService.
 *
 * Key properties:
 *  • No fallback legacy mode
 *  • No LSH-based lookup inside the server
 *  • Server-side coding for partitioned engine only
 *  • No dimension mismatch fallback
 *  • No dual-mode switch
 *  • Designed for SANNP and mSANNP (λ, m, ℓ tunable)
 *
 * The server:
 *  • encrypts
 *  • persists to RocksDB
 *  • forwards (EncryptedPoint, vector[]) to PartitionedIndexService
 *      → PartitionedIndexService computes codes and inserts
 */
public final class SecureLSHIndexService implements IndexService {

    private static final Logger log =
            LoggerFactory.getLogger(SecureLSHIndexService.class);

    // ------------------------------------------------------------
    // CORE DEPENDENCIES
    // ------------------------------------------------------------
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final RocksDBMetadataManager metadata;

    // Only engine allowed (Option-C)
    private final PartitionedIndexService engine;

    // Write-through encrypted-point buffer
    private final EncryptedPointBuffer buffer;

    // writeThrough=true means encrypt+persist to RocksDB on insert
    private volatile boolean writeThrough =
            !"false".equalsIgnoreCase(System.getProperty("index.writeThrough", "true"));

    // LSH registry (only for QueryTokenFactory compatibility)
    private final Map<Integer, EvenLSH> lshRegistry = new ConcurrentHashMap<>();
    private final SystemConfig config;

    // ------------------------------------------------------------
    // CONSTRUCTION
    // ------------------------------------------------------------

    public SecureLSHIndexService(CryptoService crypto,
                                 KeyLifeCycleService keyService,
                                 RocksDBMetadataManager metadata,
                                 PartitionedIndexService engine,
                                 EncryptedPointBuffer buffer,
                                 SystemConfig config) {

        this.crypto = (crypto != null)
                ? crypto
                : new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);

        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.config = Objects.requireNonNull(config, "config");

        log.info("SecureLSHIndexService (Option-C) initialized: partitioned-engine ENABLED.");
    }


    /** Factory from SystemConfig. Always builds PartitionedIndexService. */
    public static SecureLSHIndexService fromConfig(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            RocksDBMetadataManager metadata,
            SystemConfig cfg)
    {
        var pc = cfg.getPaper();
        if (pc == null || !pc.isEnabled()) {
            throw new IllegalStateException("System requires paper.enabled=true in config.");
        }

        PartitionedIndexService engine = new PartitionedIndexService(
                pc.getM(),
                pc.getLambda(),
                pc.getDivisions(),
                pc.getSeed()
        );

        EncryptedPointBuffer buf = createBuffer(metadata);

        return new SecureLSHIndexService(
                crypto, keyService, metadata, engine, buf, cfg
        );
    }

    private static EncryptedPointBuffer createBuffer(RocksDBMetadataManager m) {
        try {
            return new EncryptedPointBuffer(
                    Objects.requireNonNull(m.getPointsBaseDir()),
                    m
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed creating EncryptedPointBuffer", e);
        }
    }

    // ------------------------------------------------------------
    // INSERTION
    // ------------------------------------------------------------

    @Override
    public void insert(String id, double[] vector) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(vector);

        // 1) Encrypt
        EncryptedPoint ep = crypto.encrypt(id, vector);

        // 2) Persist encrypted point
        if (writeThrough) {
            persistEncryptedPoint(ep, vector.length);
        }

        // 3) Forward to paper engine (PartitionedIndexService)
        engine.insert(ep, vector);
    }

    /** Direct EncryptedPoint insertion is forbidden under Option-C. */
    @Override
    public void insert(EncryptedPoint pt) {
        throw new UnsupportedOperationException(
                "Option-C forbids insert(EncryptedPoint). Always use insert(id, vector)."
        );
    }

    private void persistEncryptedPoint(EncryptedPoint ep, int dim) {
        try {
            Map<String, String> meta = Map.of(
                    "version", String.valueOf(ep.getVersion()),
                    "dim", String.valueOf(dim)
            );

            metadata.batchUpdateVectorMetadata(Collections.singletonMap(ep.getId(), meta));
            metadata.saveEncryptedPoint(ep);
            buffer.add(ep);

            keyService.incrementOperation();
        } catch (Exception e) {
            log.error("Failed to persist {}", ep.getId(), e);
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------
    // LOOKUP
    // ------------------------------------------------------------

    public List<EncryptedPoint> lookup(QueryToken token) {
        Objects.requireNonNull(token);

        List<EncryptedPoint> raw = engine.lookup(token);
        return (raw == null) ? Collections.emptyList() : raw;
    }
    
    // ------------------------------------------------------------
    // METADATA + CACHE
    // ------------------------------------------------------------

    @Override
    public void updateCachedPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt);

        // Update engine
        engine.updateCachedPoint(pt);

        // Update RocksDB
        try {
            metadata.saveEncryptedPoint(pt);
            metadata.updateVectorMetadata(pt.getId(), Map.of(
                    "version", String.valueOf(pt.getVersion()),
                    "dim", String.valueOf(pt.getVectorLength())
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to update cached point", e);
        }
    }

    @Override
    public EncryptedPoint getEncryptedPoint(String id) {
        try {
            return metadata.loadEncryptedPoint(id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load point " + id, e);
        }
    }

    @Override
    public void delete(String id) {
        engine.delete(id);
    }

    @Override
    public int getIndexedVectorCount() {
        return engine.getTotalVectorCount();
    }

    @Override
    public Set<Integer> getRegisteredDimensions() {
        return engine.getRegisteredDimensions();
    }

    @Override
    public int getVectorCountForDimension(int dim) {
        return engine.getVectorCountForDimension(dim);
    }

    @Override
    public int getShardIdForVector(double[] vector) {
        return -1; // no shards exposed in Option-C
    }

    @Override
    public EncryptedPointBuffer getPointBuffer() {
        return buffer;
    }

    public void flushBuffers() {
        buffer.flushAll();
    }

    public void clearCache() {
        buffer.clear();
    }

    public void shutdown() {
        buffer.shutdown();
    }

    // ------------------------------------------------------------
    // LSH REGISTRY (COMPATIBILITY ONLY)
    // ------------------------------------------------------------

    public EvenLSH getLshForDimension(int dimension) {
        if (dimension <= 0) throw new IllegalArgumentException("dimension>0");

        // Partitioned system does NOT use LSH for lookup
        // but QueryTokenFactory may still require it
        return lshRegistry.computeIfAbsent(dimension, dim -> {
            int numTables = Integer.getInteger("token.lsh.tables", 8);
            long seed = Long.getLong("token.lsh.seed", 13L);
            return new EvenLSH(dim, numTables, seed);
        });
    }

    // ------------------------------------------------------------
    // WRITE-THROUGH TOGGLE
    // ------------------------------------------------------------

    public void setWriteThrough(boolean enabled) {
        this.writeThrough = enabled;
    }

    public boolean isWriteThrough() {
        return writeThrough;
    }
    @Override
    public void markDirty(int shardId) {
        // Option-C is pure partitioned mode → no legacy shards
        // Implement as explicit NO-OP
    }
}
