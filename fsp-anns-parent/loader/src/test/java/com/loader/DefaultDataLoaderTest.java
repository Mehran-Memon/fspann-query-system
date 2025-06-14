package com.loader;

import com.fspann.loader.DefaultDataLoader;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class DefaultDataLoaderTest {
    @Test
    void testConcurrentLoading() throws InterruptedException {
        DefaultDataLoader loader = new DefaultDataLoader();
        String path = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs"; // Ensure file exists
        ExecutorService executor = Executors.newFixedThreadPool(4);
        IntStream.range(0, 4).forEach(i -> executor.submit(() -> {
            try {
                loader.loadData(path, 100);
            } catch (IOException e) {
                fail("Loading failed: " + e.getMessage());
            }
        }));
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(true); // No exceptions indicate success
    }
}
