package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.common.QueryTokenV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Core data structure for encrypted LSH index.
 * Thread-safe, multi-table, per-table bucketization.
 */
public class SecureLSHIndex {
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);

    private final int numHashTables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, EncryptedPoint> points = new ConcurrentHashMap<>();
    // One map per table: bucketId -> list of points in that bucket
    private final List<Map<Integer, CopyOnWriteArrayList<EncryptedPoint>>> tables = new ArrayList<>();

    private final EvenLSH lsh;

    public SecureLSHIndex(int numHashTables, int numBuckets, EvenLSH lsh) {
        if (numHashTables <= 0) throw new IllegalArgumentException("numHashTables must be > 0");
        if (numBuckets <= 0) throw new IllegalArgumentException("numBuckets must be > 0");
        if (numBuckets != lsh.getNumBuckets()) {
            logger.warn("numBuckets={} differs from lsh.getNumBuckets()={}", numBuckets, lsh.getNumBuckets());
        }
        this.numHashTables = numHashTables;
        this.lsh = Objects.requireNonNull(lsh, "LSH function cannot be null");
        for (int i = 0; i < numHashTables; i++) {
            tables.add(new ConcurrentHashMap<>());
        }
    }

    public void addPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        List<Integer> perTable = Objects.requireNonNull(pt.getBuckets(), "EncryptedPoint.buckets must not be null");
        if (perTable.size() != numHashTables) {
            throw new IllegalArgumentException("EncryptedPoint.buckets size must equal numHashTables");
        }

        lock.writeLock().lock();
        try {
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

    /** Caller must hold write lock. */
    private void internalRemoveFromTables(EncryptedPoint pt) {
        List<Integer> perTable = pt.getBuckets();
        for (int t = 0; t < numHashTables; t++) {
            int b = perTable.get(t);
            CopyOnWriteArrayList<EncryptedPoint> bucket = tables.get(t).get(b);
            if (bucket != null) bucket.remove(pt);
        }
    }


    /** Query with per-table bucket expansions from the token. Deduplicates across tables. */
    public List<EncryptedPoint> queryEncrypted(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        lock.readLock().lock();
        try {
            Set<EncryptedPoint> result = new LinkedHashSet<>();
            List<List<Integer>> tableBuckets = token.getTableBuckets();
            int tablesToUse = Math.min(token.getNumTables(), numHashTables);

            if (tableBuckets.size() != tablesToUse) {
                throw new IllegalArgumentException("tableBuckets size must equal numTables in token");
            }

            for (int t = 0; t < tablesToUse; t++) {
                Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table = tables.get(t);
                for (Integer b : tableBuckets.get(t)) {
                    CopyOnWriteArrayList<EncryptedPoint> bucket = table.get(b);
                    if (bucket != null) result.addAll(bucket);
                }
            }
            logger.debug("Query returned {} candidates for dim={} perTableExpansions={}",
                    result.size(), token.getDimension(), tableBuckets);
            return new ArrayList<>(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Lightweight union count for fanout evaluation (per-table expansions). */
    public int candidateCount(List<List<Integer>> tableBuckets) {
        lock.readLock().lock();
        try {
            Set<EncryptedPoint> out = new LinkedHashSet<>();
            int T = Math.min(tableBuckets.size(), numHashTables);
            for (int t = 0; t < T; t++) {
                Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table = tables.get(t);
                for (Integer b : tableBuckets.get(t)) {
                    CopyOnWriteArrayList<EncryptedPoint> l = table.get(b);
                    if (l != null) out.addAll(l);
                }
            }
            return out.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getNumHashTables() { return numHashTables; }
    public EvenLSH getLsh() { return lsh; }
    public int getPointCount() { return points.size(); }
    public boolean hasPoint(String id) { return points.containsKey(id); }
    public EncryptedPoint getPoint(String id) { return points.get(id); }
}
