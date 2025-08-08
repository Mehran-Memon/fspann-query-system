package com.fspann.common;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Path;

public class RocksDBTestCleaner {
    public static void clean(RocksDBMetadataManager mgr) {
        if (mgr != null) {
            try {
                mgr.logStats();
                mgr.printSummary();
            } catch (Exception e) {
            }
        }
    }


    public static void destroy(Path dbPath) {
        try (Options opt = new Options().setCreateIfMissing(true)) {
            RocksDB.destroyDB(dbPath.toString(), opt);
        } catch (Exception e) {
            System.err.println("Failed to destroy RocksDB: " + e.getMessage());
        }
    }
}

