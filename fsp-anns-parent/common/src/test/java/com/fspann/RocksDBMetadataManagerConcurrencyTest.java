package com.fspann;

import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("RocksDBMetadataManager Concurrency Tests")
public class RocksDBMetadataManagerConcurrencyTest {

    private RocksDBMetadataManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        Path metadataPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");
        Files.createDirectories(metadataPath);
        Files.createDirectories(pointsPath);

        manager = RocksDBMetadataManager.create(metadataPath.toString(), pointsPath.toString());
    }

    @Test
    @DisplayName("Test concurrent metadata updates")
    public void testConcurrentUpdates() throws InterruptedException {
        int numThreads = 10;
        int itemsPerThread = 100;

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    String id = "v-" + threadId + "-" + i;
                    Map<String, String> meta = new HashMap<>();
                    meta.put("thread", String.valueOf(threadId));
                    meta.put("index", String.valueOf(i));
                    manager.updateVectorMetadata(id, meta);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        List<String> ids = manager.getAllVectorIds();
        assertEquals(numThreads * itemsPerThread, ids.size());
    }
}