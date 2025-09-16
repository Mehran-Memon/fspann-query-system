package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LEGACY: In-memory multi-table LSH index (multiprobe bucket union).
 * -----------------------------------------------------------------------------
 * This structure powers the older EvenLSH + multi-probe path, where queries
 * specify per-table bucket expansions and we return the union of candidates.
 *
 * The default, paper-aligned path for SANNP/mSANNP should NOT use this class.
 * Instead, use:
 *   Coding (Algorithm-1) -> Greedy partition (Algorithm-2) -> Tag query (Algorithm-3)
 * which returns ≤ 2w subset files per division, then client-side kNN.
 *
 * Thread-safety:
 *  - Point map and per-table bucket maps are concurrent.
 *  - Structural mutations guarded by a RW lock (write for insert/remove, read for lookup).
 *
 * Notes:
 *  - We filter legacy fake points (IDs prefixed "FAKE_") out of query results.
 *  - We de-duplicate by point ID across tables/buckets.
 */
@Deprecated
public class SecureLSHIndex {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);

    /** Number of hash tables maintained by this index. */
    private final int numHashTables;

    /** Global lock for structural safety across tables map + points map. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** All known points by ID (latest version only). */
    private final Map<String, EncryptedPoint> points = new ConcurrentHashMap<>();

    /**
     * Per-table bucket directory:
     *   tables.get(t) : Map<bucketId, CopyOnWriteArrayList<EncryptedPoint>>
     *
     * CopyOnWriteArrayList favors concurrent read-heavy queries; inserts/removes are rarer.
     */
    private final List<Map<Integer, CopyOnWriteArrayList<EncryptedPoint>>> tables = new ArrayList<>();

    /** Legacy LSH router used only to validate bucket dimensions / debugging. */
    private final EvenLSH lsh;

    // ----------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------
    public SecureLSHIndex(int numHashTables, int numBuckets, EvenLSH lsh) {
        if (numHashTables <= 0) throw new IllegalArgumentException("numHashTables must be > 0");
        if (numBuckets <= 0) throw new IllegalArgumentException("numBuckets must be > 0");
        this.lsh = Objects.requireNonNull(lsh, "LSH function cannot be null");
        if (numBuckets != lsh.getNumBuckets()) {
            logger.warn("numBuckets={} differs from lsh.getNumBuckets()={}", numBuckets, lsh.getNumBuckets());
        }
        this.numHashTables = numHashTables;
        for (int i = 0; i < numHashTables; i++) {
            tables.add(new ConcurrentHashMap<>());
        }
        logger.info("Initialized SecureLSHIndex tables={}, buckets={}, dims={}",
                numHashTables, numBuckets, lsh.getDimensions());
    }

    // ----------------------------------------------------------------------------
    // Mutations
    // ----------------------------------------------------------------------------

    /**
     * Insert or replace a point. The point MUST carry per-table bucket IDs
     * whose size equals numHashTables.
     */
    public void addPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        List<Integer> perTable = Objects.requireNonNull(pt.getBuckets(), "EncryptedPoint.buckets must not be null");
        if (perTable.size() != numHashTables) {
            throw new IllegalArgumentException("EncryptedPoint.buckets size must equal numHashTables");
        }

        lock.writeLock().lock();
        try {
            // If this ID existed, remove old placements first
            EncryptedPoint old = points.remove(pt.getId());
            if (old != null) {
                internalRemoveFromTables(old);
                logger.debug("Point {} existed; replaced.", pt.getId());
            }

            points.put(pt.getId(), pt);
            for (int t = 0; t < numHashTables; t++) {
                int b = perTable.get(t);
                tables.get(t).computeIfAbsent(b, k -> new CopyOnWriteArrayList<>()).add(pt);
            }
            logger.debug("Inserted EncryptedPoint id={} into {} tables", pt.getId(), numHashTables);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Remove a point by ID (no-op if not present). */
    public void removePoint(String id) {
        Objects.requireNonNull(id, "Point ID cannot be null");
        lock.writeLock().lock();
        try {
            EncryptedPoint pt = points.remove(id);
            if (pt != null) {
                internalRemoveFromTables(pt);
                logger.debug("Removed point {} from index", id);
            } else {
                logger.debug("Attempted to remove nonexistent point {}", id);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Caller must hold the write lock. Cleans up empty buckets too. */
    private void internalRemoveFromTables(EncryptedPoint pt) {
        List<Integer> perTable = pt.getBuckets();
        for (int t = 0; t < numHashTables; t++) {
            int b = perTable.get(t);
            Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table = tables.get(t);
            CopyOnWriteArrayList<EncryptedPoint> bucket = table.get(b);
            if (bucket != null) {
                bucket.remove(pt);
                if (bucket.isEmpty()) table.remove(b); // free empty bucket
            }
        }
    }

    // ----------------------------------------------------------------------------
    // Queries (legacy multiprobe)
    // ----------------------------------------------------------------------------

    /**
     * Query using per-table bucket expansions provided by the token (or legacy
     * replicated buckets). Returns the union of candidates across all tables.
     *
     * De-duplicates by ID and filters out legacy fake points (ID starts with "FAKE_").
     */
    public List<EncryptedPoint> queryEncrypted(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        lock.readLock().lock();
        try {
            // Resolve per-table bucket lists
            List<List<Integer>> tableBuckets = token.hasPerTable()
                    ? token.getTableBuckets()
                    : token.getTableBucketsOrLegacy(numHashTables);

            int tablesToUse = Math.min(token.getNumTables(), numHashTables);
            if (tablesToUse <= 0) return Collections.emptyList();

            if (tableBuckets.size() < tablesToUse) {
                throw new IllegalArgumentException("tableBuckets size must equal numTables in token");
            }

            // Deduplicate by ID (logical uniqueness), then materialize in insertion order
            Set<String> seen = new LinkedHashSet<>();
            List<EncryptedPoint> results = new ArrayList<>();

            for (int t = 0; t < tablesToUse; t++) {
                Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table = tables.get(t);
                for (Integer b : tableBuckets.get(t)) {
                    CopyOnWriteArrayList<EncryptedPoint> bucket = table.get(b);
                    if (bucket == null) continue;
                    for (EncryptedPoint pt : bucket) {
                        // Filter legacy fake points from candidate sets
                        if (pt.getId() != null && pt.getId().startsWith("FAKE_")) continue;
                        if (seen.add(pt.getId())) {
                            results.add(pt);
                        }
                    }
                }
            }

            logger.debug("Query returned {} candidates for dim={} perTableExpansions={}",
                    results.size(), token.getDimension(), tableBuckets);
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Lightweight union-count helper for fanout evaluation (per-table expansions). */
    public int candidateCount(List<List<Integer>> tableBuckets) {
        lock.readLock().lock();
        try {
            Set<String> ids = new LinkedHashSet<>();
            int T = Math.min(tableBuckets.size(), numHashTables);
            for (int t = 0; t < T; t++) {
                Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table = tables.get(t);
                for (Integer b : tableBuckets.get(t)) {
                    CopyOnWriteArrayList<EncryptedPoint> list = table.get(b);
                    if (list == null) continue;
                    for (EncryptedPoint pt : list) {
                        if (pt.getId() != null && pt.getId().startsWith("FAKE_")) continue;
                        ids.add(pt.getId());
                    }
                }
            }
            return ids.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    // ----------------------------------------------------------------------------
    // Accessors / diagnostics
    // ----------------------------------------------------------------------------
    public int getNumHashTables() { return numHashTables; }
    public EvenLSH getLsh() { return lsh; }
    public int getPointCount() { return points.size(); }
    public boolean hasPoint(String id) { return points.containsKey(id); }
    public EncryptedPoint getPoint(String id) { return points.get(id); }

    /** Debug helper: returns a snapshot of bucket sizes per table. */
    public Map<Integer, Map<Integer, Integer>> dumpBucketSizes() {
        lock.readLock().lock();
        try {
            Map<Integer, Map<Integer, Integer>> out = new LinkedHashMap<>();
            for (int t = 0; t < numHashTables; t++) {
                Map<Integer, Integer> sizes = new TreeMap<>();
                for (Map.Entry<Integer, CopyOnWriteArrayList<EncryptedPoint>> e : tables.get(t).entrySet()) {
                    sizes.put(e.getKey(), e.getValue().size());
                }
                out.put(t, sizes);
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }
}
