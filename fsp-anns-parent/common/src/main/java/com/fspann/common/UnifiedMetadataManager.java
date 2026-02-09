package com.fspann.common;

import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class UnifiedMetadataManager implements MetadataManager {
    private final RocksDBMetadataManager rocksDBMetadataManager;
    private final ShardedMetadataManager shardedMetadataManager;
    private final boolean useSharding;

    // Constructor to choose between RocksDB and Sharded Metadata Managers
    public UnifiedMetadataManager(String dbPath, String pointsPath, int numShards, boolean useSharding) throws IOException, RocksDBException {
        this.useSharding = useSharding;

        if (useSharding) {
            // Initialize ShardedMetadataManager
            shardedMetadataManager = new ShardedMetadataManager(dbPath, numShards, pointsPath);
            rocksDBMetadataManager = null;
        } else {
            // Initialize RocksDBMetadataManager
            rocksDBMetadataManager = RocksDBMetadataManager.create(dbPath, pointsPath);
            shardedMetadataManager = null;
        }
    }

    // ===== Single Vector Metadata =====

    @Override
    public void putVectorMetadata(String vectorId, Map<String, String> metadataMap) {
        if (useSharding) {
            shardedMetadataManager.putVectorMetadata(vectorId, metadataMap);
        } else {
            rocksDBMetadataManager.putVectorMetadata(vectorId, metadataMap);
        }
    }

    @Override
    public Map<String, String> getVectorMetadata(String vectorId) {
        if (useSharding) {
            return shardedMetadataManager.getVectorMetadata(vectorId);
        } else {
            return rocksDBMetadataManager.getVectorMetadata(vectorId);
        }
    }

    @Override
    public void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        if (useSharding) {
            shardedMetadataManager.updateVectorMetadata(vectorId, updates);
        } else {
            rocksDBMetadataManager.updateVectorMetadata(vectorId, updates);
        }
    }

    // ===== Batch Metadata Operations =====

    @Override
    public void batchUpdateVectorMetadata(Map<String, Map<String, String>> updates) throws IOException {
        if (useSharding) {
            shardedMetadataManager.batchUpdateVectorMetadata(updates);
        } else {
            rocksDBMetadataManager.batchUpdateVectorMetadata(updates);
        }
    }

    @Override
    public List<String> getAllVectorIds() {
        if (useSharding) {
            return shardedMetadataManager.getAllVectorIds();
        } else {
            return rocksDBMetadataManager.getAllVectorIds();
        }
    }

    // ===== Encrypted Point Persistence =====

    @Override
    public List<EncryptedPoint> getAllEncryptedPoints() {
        if (useSharding) {
            return shardedMetadataManager.getAllEncryptedPoints();
        } else {
            return rocksDBMetadataManager.getAllEncryptedPoints();
        }
    }

    @Override
    public void saveEncryptedPoint(EncryptedPoint point) throws IOException {
        if (useSharding) {
            shardedMetadataManager.saveEncryptedPoint(point);
        } else {
            rocksDBMetadataManager.saveEncryptedPoint(point);
        }
    }

    @Override
    public void saveEncryptedPointsBatch(Collection<EncryptedPoint> points) throws IOException {
        if (useSharding) {
            shardedMetadataManager.saveEncryptedPointsBatch(points);
        } else {
            rocksDBMetadataManager.saveEncryptedPointsBatch(points);
        }
    }

    @Override
    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        if (useSharding) {
            return shardedMetadataManager.loadEncryptedPoint(id);
        } else {
            return rocksDBMetadataManager.loadEncryptedPoint(id);
        }
    }

    // ===== Lifecycle =====

    @Override
    public void flush() {
        if (useSharding) {
            shardedMetadataManager.flush();
        } else {
            rocksDBMetadataManager.flush();
        }
    }

    @Override
    public void shutdown() {
        if (useSharding) {
            shardedMetadataManager.shutdown();
        } else {
            rocksDBMetadataManager.shutdown();
        }
    }

    @Override
    public void close() {
        if (useSharding) {
            shardedMetadataManager.close();
        } else {
            rocksDBMetadataManager.close();
        }
    }

    @Override
    public StorageMetrics getStorageMetrics() {
        if (useSharding) {
            return shardedMetadataManager.getStorageMetrics();
        } else {
            return rocksDBMetadataManager.getStorageMetrics();
        }
    }

    @Override
    public void saveIndexVersion(int version) {
        if (useSharding) {
            shardedMetadataManager.saveIndexVersion(version);
        } else {
            rocksDBMetadataManager.saveIndexVersion(version);
        }
    }

    @Override
    public void printSummary() {
        if (useSharding) {
            shardedMetadataManager.printSummary();
        } else {
            rocksDBMetadataManager.printSummary();
        }
    }

    @Override
    public void logStats() {
        if (useSharding) {
            shardedMetadataManager.logStats();
        } else {
            rocksDBMetadataManager.logStats();
        }
    }
// ===== New Interface Methods Delegation =====

    @Override
    public long sizePointsDir() {
        if (useSharding) {
            return shardedMetadataManager.sizePointsDir();
        } else {
            return rocksDBMetadataManager.sizePointsDir();
        }
    }

    @Override
    public int countWithVersion(int keyVersion) throws IOException {
        if (useSharding) {
            return shardedMetadataManager.countWithVersion(keyVersion);
        } else {
            return rocksDBMetadataManager.countWithVersion(keyVersion);
        }
    }

    @Override
    public int getVersionOfVector(String id) {
        if (useSharding) {
            return shardedMetadataManager.getVersionOfVector(id);
        } else {
            return rocksDBMetadataManager.getVersionOfVector(id);
        }
    }

}
