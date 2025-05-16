package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
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
    private final Map<String,EncryptedPoint> points = new ConcurrentHashMap<>();
    private final List<Map<Integer,CopyOnWriteArrayList<EncryptedPoint>>> tables = new ArrayList<>();
    private final Set<Integer> dirtyShards = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public SecureLSHIndex(int numHashTables, int numShards) {
        this.numHashTables = numHashTables;
        this.numShards     = numShards;
        for(int i=0;i<numHashTables;i++){
            tables.add(new ConcurrentHashMap<>());
        }
    }

    public void addPoint(EncryptedPoint pt) {
        lock.writeLock().lock();
        try {
            points.put(pt.getId(), pt);
            int shard = pt.getShardId();
            for(Map<Integer,CopyOnWriteArrayList<EncryptedPoint>> table: tables){
                table.computeIfAbsent(shard, k->new CopyOnWriteArrayList<>()).add(pt);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removePoint(String id) {
        lock.writeLock().lock();
        try {
            EncryptedPoint pt = points.remove(id);
            if(pt!=null){
                int shard = pt.getShardId();
                for(Map<Integer,CopyOnWriteArrayList<EncryptedPoint>> table: tables){
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
            List<Integer> buckets = token.getCandidateBuckets();
            int tablesToUse = Math.min(token.getNumTables(), numHashTables);
            for(int t=0;t<tablesToUse;t++){
                for(int b: buckets){
                    result.addAll(tables.get(t).getOrDefault(b, new CopyOnWriteArrayList<>()));
                }
            }
            List<EncryptedPoint> list = new ArrayList<>(result);
            list.sort(Comparator.comparing(EncryptedPoint::getId)); // stable order
            return list.subList(0, Math.min(token.getTopK(), list.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void markShardDirty(int shardId){ dirtyShards.add(shardId); }
    public Set<Integer> getDirtyShards(){ return Collections.unmodifiableSet(dirtyShards); }
}
