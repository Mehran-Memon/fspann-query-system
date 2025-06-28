package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import javax.crypto.SecretKey;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
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
        lock.writeLock().lock();
        try {
            points.put(pt.getId(), pt);
            List<Future<?>> futures = new ArrayList<>();
            for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                final Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> finalTable = table;
                futures.add(executor.submit(() -> finalTable.computeIfAbsent(pt.getShardId(), k -> new CopyOnWriteArrayList<>()).add(pt)));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    logger.error("Thread interrupted while adding point", e);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during point addition", e);
                } catch (ExecutionException e) {
                    logger.error("Execution failed while adding point", e);
                    throw new RuntimeException("Failed to add point due to execution error", e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removePoint(String id) {
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
        lock.readLock().lock();
        try {
            Set<EncryptedPoint> result = new LinkedHashSet<>();
            List<Integer> buckets = token.getBuckets();
            int tablesToUse = Math.min(token.getNumTables(), numHashTables);
            for (int t = 0; t < tablesToUse; t++) {
                for (Integer b : buckets) {
                    result.addAll(tables.get(t).getOrDefault(b, new CopyOnWriteArrayList<>()));
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

    public int getDimensions() {
        return lsh.getDimensions();
    }

    public EvenLSH getLsh() {
        return lsh;
    }

    public void recomputeBuckets(List<EncryptedPoint> points, CryptoService crypto, SecretKey key) {
        lock.writeLock().lock();
        try {
            List<Double> projections = new ArrayList<>(points.size());
            Map<String, Double> idToProjection = new HashMap<>();

            for (EncryptedPoint pt : points) {
                double[] vec = crypto.decryptFromPoint(pt, key);
                double projection = lsh.project(vec);
                projections.add(projection);
                idToProjection.put(pt.getId(), projection);
            }

            double[] boundaries = LSHUtils.quantiles(projections, numShards);

            for (EncryptedPoint pt : points) {
                double projection = idToProjection.get(pt.getId());
                int newShardId = findBucket(projection, boundaries);
                if (newShardId != pt.getShardId()) {
                    for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                        table.getOrDefault(pt.getShardId(), new CopyOnWriteArrayList<>()).remove(pt);
                    }
                    EncryptedPoint updatedPt = new EncryptedPoint(pt.getId(), newShardId, pt.getIv(), pt.getCiphertext(), pt.getVersion(), pt.getVectorLength());
                    this.points.put(updatedPt.getId(), updatedPt);
                    List<Future<?>> futures = new ArrayList<>();
                    for (Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> table : tables) {
                        final Map<Integer, CopyOnWriteArrayList<EncryptedPoint>> finalTable = table;
                        futures.add(executor.submit(() -> finalTable.computeIfAbsent(newShardId, k -> new CopyOnWriteArrayList<>()).add(updatedPt)));
                    }
                    for (Future<?> f : futures) {
                        try {
                            f.get();
                        } catch (InterruptedException e) {
                            logger.error("Thread interrupted while updating bucket", e);
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted during bucket update", e);
                        } catch (ExecutionException e) {
                            logger.error("Execution failed while updating bucket", e);
                            throw new RuntimeException("Failed to update bucket due to execution error", e);
                        }
                    }
                    markShardDirty(newShardId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int findBucket(double projection, double[] boundaries) {
        for (int i = 0; i < boundaries.length; i++) {
            if (projection <= boundaries[i]) {
                return i;
            }
        }
        return boundaries.length;
    }

    public int getPointCount() {
        return points.size();
    }



}
