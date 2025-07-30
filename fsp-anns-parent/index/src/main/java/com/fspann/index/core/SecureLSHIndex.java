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
        this.lsh = Objects.requireNonNull(lsh, "LSH function cannot be null");

        for (int i = 0; i < numHashTables; i++) {
            tables.add(new ConcurrentHashMap<>());
        }
    }

    public void addPoint(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        lock.writeLock().lock();
        try {
            if (points.containsKey(pt.getId())) {
                logger.debug("Point {} already exists, replacing...", pt.getId());
                removePoint(pt.getId());  // ensure clean overwrite
            }
            points.put(pt.getId(), pt);
            for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                table.computeIfAbsent(pt.getShardId(), k -> new CopyOnWriteArrayList<>()).add(pt);
            }
            logger.debug("Inserted EncryptedPoint id={} into shard {}", pt.getId(), pt.getShardId());
        } catch (Exception e) {
            logger.error("Failed to add point {}", pt.getId(), e);
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
                    CopyOnWriteArrayList<EncryptedPoint> bucket = table.get(shard);
                    if (bucket != null) {
                        bucket.remove(pt);
                        logger.debug("Removed point {} from shard {}", id, shard);
                    }
                }
            } else {
                logger.debug("Attempted to remove nonexistent point {}", id);
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
                for (Integer bucketId : buckets) {
                    CopyOnWriteArrayList<EncryptedPoint> bucket = tables.get(t).get(bucketId);
                    if (bucket != null) {
                        result.addAll(bucket);
                    } else {
                        logger.trace("Empty bucket {} in table {}", bucketId, t);
                    }
                }
            }

            logger.debug("Query returned {} candidates for dim={} tokenBuckets={}", result.size(), token.getQueryVector().length, buckets);
            return new ArrayList<>(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void markShardDirty(int shardId) {
        dirtyShards.add(shardId);
        logger.debug("Marked shard {} as dirty", shardId);
    }

    public Set<Integer> getDirtyShards() {
        return Collections.unmodifiableSet(dirtyShards);
    }

    public void clearDirtyShard(int shardId) {
        dirtyShards.remove(shardId);
        logger.debug("Cleared dirty shard {}", shardId);
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

    public boolean hasPoint(String id) {
        return points.containsKey(id);
    }

    public EncryptedPoint getPoint(String id) {
        return points.get(id);
    }
}
