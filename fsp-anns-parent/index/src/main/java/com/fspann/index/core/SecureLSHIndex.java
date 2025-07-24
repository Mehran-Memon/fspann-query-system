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
 * Core data structure for encrypted LSH index.
 * Thread-safe, shard-based, multi-table.
 */
public class SecureLSHIndex {
    private final int numHashTables;
    private final int numShards;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, EncryptedPoint> points = new ConcurrentHashMap<>();
    private final List<Map<Integer, CopyOnWriteArrayList<EncryptedPoint>>> tables = new ArrayList<>();
    private final Set<Integer> dirtyShards = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Logger logger = LoggerFactory.getLogger(SecureLSHIndex.class);
    private final EvenLSH lsh;

    public SecureLSHIndex(int numHashTables, int numShards, EvenLSH lsh) {
        this.numHashTables = numHashTables;
        this.numShards = numShards;
        this.lsh = lsh;
        for (int i = 0; i < numHashTables; i++) {
            tables.add(new ConcurrentHashMap<>());
        }
    }

    public void addPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        lock.writeLock().lock();
        try {
            points.put(pt.getId(), pt);
            for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                table.computeIfAbsent(pt.getShardId(), k -> new CopyOnWriteArrayList<>()).add(pt);
            }
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
                int shard = pt.getShardId();
                for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                    table.getOrDefault(shard, new CopyOnWriteArrayList<>()).remove(pt);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<EncryptedPoint> queryEncrypted(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        lock.readLock().lock();
        try {
            Set<EncryptedPoint> result = new LinkedHashSet<>();
            List<Integer> buckets = token.getBuckets();
            int tablesToUse = Math.min(token.getNumTables(), numHashTables);
            for (int t = 0; t < tablesToUse; t++) {
                for (Integer b : buckets) {
                    CopyOnWriteArrayList<EncryptedPoint> bucket = tables.get(t).get(b);
                    if (bucket != null) {
                        result.addAll(bucket);
                    }
                }
            }
            return new ArrayList<>(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void markShardDirty(int shardId) {
        dirtyShards.add(shardId);
    }

    public Set<Integer> getDirtyShards() {
        return Collections.unmodifiableSet(dirtyShards);
    }

    public void clearDirtyShard(int shardId) {
        dirtyShards.remove(shardId);
    }

    public int getNumHashTables() {
        return numHashTables;
    }

    public EvenLSH getLsh() {
        return lsh;
    }

    public int getPointCount() {
        return points.size();
    }
}