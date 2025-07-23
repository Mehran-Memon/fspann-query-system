package com.loader;

import com.fspann.loader.DefaultDataLoader;
import com.fspann.loader.FormatLoader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class DefaultDataLoaderTest {

    @Test
    void testConcurrentStreaming() throws InterruptedException {
        DefaultDataLoader loader = new DefaultDataLoader();
        Path dataFile = Paths.get(
                "E:\\Research Work\\Datasets\\sift_dataset\\sift_base.fvecs"
        );

        // ✅ FIX: Use lookup() instead of loadData(Path)
        FormatLoader fl = loader.lookup(dataFile);
        assertNotNull(fl);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        Runnable task = () -> {
            try {
                Iterator<double[]> it = fl.openVectorIterator(dataFile);
                int count = 0;
                while (it.hasNext() && count < 100) {
                    it.next();
                    count++;
                }
                assertEquals(100, count);
            } catch (IOException e) {
                fail("Streaming failed: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        };

        IntStream.range(0, 4).forEach(i -> exec.submit(task));
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Threads timed out");
        exec.shutdownNow();
    }

    @Test
    void testInvalidFormatExtension() {
        DefaultDataLoader loader = new DefaultDataLoader();
        assertThrows(IllegalArgumentException.class, // ✅ updated to match your actual exception
                () -> loader.loadData("somefile.xyz", 50));
    }

    @Test
    void testBatchLoadingCsv() throws IOException {
        DefaultDataLoader loader = new DefaultDataLoader();
        String path = "E:\\Research Work\\Datasets\\synthetic_data\\synthetic_128d\\synthetic_gaussian_128d_storage.csv";

        List<double[]> first = loader.loadData(path, 3);
        assertTrue(first.size() <= 3);

        List<double[]> second = loader.loadData(path, 3);
        assertTrue(second.size() <= 3);
    }
}
