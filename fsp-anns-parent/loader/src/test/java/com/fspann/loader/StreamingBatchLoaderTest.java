package com.fspann.loader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

@DisplayName("StreamingBatchLoader Unit Tests")
public class StreamingBatchLoaderTest {

    @Test
    @DisplayName("Test StreamingBatchLoader returns correct batch size")
    public void testBatchSizeCorrect() {
        List<double[]> data = createMockVectors(100);
        Iterator<double[]> it = data.iterator();

        StreamingBatchLoader loader = new StreamingBatchLoader(it, 10);
        List<double[]> batch = loader.nextBatch();

        assertEquals(10, batch.size());
    }

    @Test
    @DisplayName("Test StreamingBatchLoader returns remaining vectors")
    public void testPartialBatch() {
        List<double[]> data = createMockVectors(25);
        Iterator<double[]> it = data.iterator();

        StreamingBatchLoader loader = new StreamingBatchLoader(it, 10);

        List<double[]> batch1 = loader.nextBatch();
        assertEquals(10, batch1.size());

        List<double[]> batch2 = loader.nextBatch();
        assertEquals(10, batch2.size());

        List<double[]> batch3 = loader.nextBatch();
        assertEquals(5, batch3.size());
    }

    @Test
    @DisplayName("Test StreamingBatchLoader returns empty list at EOF")
    public void testEmptyBatchAtEOF() {
        List<double[]> data = createMockVectors(10);
        Iterator<double[]> it = data.iterator();

        StreamingBatchLoader loader = new StreamingBatchLoader(it, 10);

        List<double[]> batch1 = loader.nextBatch();
        assertEquals(10, batch1.size());

        List<double[]> batch2 = loader.nextBatch();
        assertTrue(batch2.isEmpty());
    }

    @Test
    @DisplayName("Test StreamingBatchLoader with single vector")
    public void testSingleVectorBatch() {
        List<double[]> data = createMockVectors(1);
        Iterator<double[]> it = data.iterator();

        StreamingBatchLoader loader = new StreamingBatchLoader(it, 100);
        List<double[]> batch = loader.nextBatch();

        assertEquals(1, batch.size());
    }

    private List<double[]> createMockVectors(int count) {
        List<double[]> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            vectors.add(new double[128]);
        }
        return vectors;
    }
}