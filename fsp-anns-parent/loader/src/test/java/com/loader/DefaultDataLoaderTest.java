package com.loader;

import com.fspann.loader.DefaultDataLoader;
import com.fspann.loader.FormatLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class DefaultDataLoaderTest {

    @TempDir
    Path tmp;

    private Path writeFvecs(Path path, int numVecs, int dim) throws IOException {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
            for (int n = 0; n < numVecs; n++) {
                // FVECS is little-endian: write dim then floats (all LE)
                out.writeInt(Integer.reverseBytes(dim));
                for (int i = 0; i < dim; i++) {
                    int bits = Float.floatToIntBits((float) (n + i));
                    out.writeInt(Integer.reverseBytes(bits));
                }
            }
        }
        return path;
    }

    private Path writeCsv(Path path, int numVecs, int dim) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int n = 0; n < numVecs; n++) {
            for (int i = 0; i < dim; i++) {
                if (i > 0) sb.append(',');
                sb.append(n + i + 0.5);
            }
            sb.append('\n');
        }
        Files.writeString(path, sb.toString());
        return path;
    }

    @Test
    void testConcurrentStreaming() throws Exception {
        DefaultDataLoader loader = new DefaultDataLoader();

        Path fvecs = writeFvecs(tmp.resolve("base.fvecs"), /*numVecs*/ 500, /*dim*/ 4);

        // Each thread opens its own iterator and reads 100 vectors
        FormatLoader fl = loader.lookup(fvecs);
        assertNotNull(fl);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        Runnable task = () -> {
            try {
                Iterator<double[]> it = fl.openVectorIterator(fvecs);
                int count = 0;
                while (it.hasNext() && count < 100) {
                    double[] v = it.next();
                    assertEquals(4, v.length);
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
        assertThrows(IllegalArgumentException.class,
                () -> loader.loadData(tmp.resolve("somefile.xyz").toString(), 50));
    }

    @Test
    void testBatchLoadingCsv() throws IOException {
        DefaultDataLoader loader = new DefaultDataLoader();
        Path csv = writeCsv(tmp.resolve("data.csv"), /*numVecs*/ 8, /*dim*/ 3);

        List<double[]> first = loader.loadData(csv.toString(), 3);
        assertTrue(first.size() <= 3);
        assertFalse(first.isEmpty());
        int firstSize = first.size();

        List<double[]> second = loader.loadData(csv.toString(), 3);
        assertTrue(second.size() <= 3);
        assertFalse(second.isEmpty());

        // Consume remaining
        List<double[]> third = loader.loadData(csv.toString(), 3);
        List<double[]> fourth = loader.loadData(csv.toString(), 3); // should be empty now

        assertTrue(fourth.isEmpty(), "Iterator should be exhausted by now");

        // Basic shape checks
        first.forEach(v -> assertEquals(3, v.length));
        second.forEach(v -> assertEquals(3, v.length));
        third.forEach(v -> assertEquals(3, v.length));

        // Total read equals 8
        int total = first.size() + second.size() + third.size();
        assertEquals(8, total);
    }
}
