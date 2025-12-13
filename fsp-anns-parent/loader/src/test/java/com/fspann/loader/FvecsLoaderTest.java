package com.fspann.loader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("FVECS Loader Unit Tests")
public class FvecsLoaderTest {

    private FvecsLoader loader;
    private Path testFile;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        loader = new FvecsLoader();
    }

    @Test
    @DisplayName("Test FVECS loader reads single vector correctly")
    public void testReadSingleVector() throws Exception {
        testFile = createFvecsFile(1, 128);

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertTrue(it.hasNext());

        double[] v = it.next();
        assertNotNull(v);
        assertEquals(128, v.length);
        assertFalse(it.hasNext());
    }

    @Test
    @DisplayName("Test FVECS loader reads multiple vectors")
    public void testReadMultipleVectors() throws Exception {
        testFile = createFvecsFile(10, 128);

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        int count = 0;
        while (it.hasNext()) {
            double[] v = it.next();
            assertEquals(128, v.length);
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    @DisplayName("Test FVECS loader with large vectors")
    public void testLargeVectors() throws Exception {
        testFile = createFvecsFile(5, 1000);

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertTrue(it.hasNext());
        double[] v = it.next();
        assertEquals(1000, v.length);
    }

    @Test
    @DisplayName("Test FVECS loader vector values are preserved")
    public void testVectorValuesPreserved() throws Exception {
        testFile = tempDir.resolve("test.fvecs");
        double[] expected = {1.5, 2.5, 3.5, 4.5};
        int dim = expected.length;

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(testFile)))) {

            dos.writeInt(Integer.reverseBytes(dim));
            for (double v : expected) {
                dos.writeInt(Integer.reverseBytes(Float.floatToIntBits((float) v)));
            }
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        double[] read = it.next();

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], read[i], 0.001);
        }
    }

    @Test
    @DisplayName("Test FVECS loader rejects invalid dimension (0)")
    public void testInvalidDimensionZero() throws Exception {
        testFile = tempDir.resolve("invalid.fvecs");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(Integer.reverseBytes(0));
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertThrows(UncheckedIOException.class, it::next);
    }

    @Test
    @DisplayName("Test FVECS loader rejects invalid dimension (negative)")
    public void testInvalidDimensionNegative() throws Exception {
        testFile = tempDir.resolve("invalid.fvecs");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(Integer.reverseBytes(-10));
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertThrows(UncheckedIOException.class, it::next);
    }

    @Test
    @DisplayName("Test FVECS loader rejects dimension > 1M")
    public void testInvalidDimensionTooLarge() throws Exception {
        testFile = tempDir.resolve("invalid.fvecs");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(Integer.reverseBytes(2_000_000));
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertThrows(UncheckedIOException.class, it::next);
    }

    @Test
    @DisplayName("Test FVECS index iterator throws UnsupportedOperationException")
    public void testIndexIteratorUnsupported() throws Exception {
        testFile = createFvecsFile(1, 128);
        assertThrows(UnsupportedOperationException.class,
                () -> loader.openIndexIterator(testFile));
    }

    @Test
    @DisplayName("Test FVECS loader handles empty file gracefully")
    public void testEmptyFile() throws Exception {
        testFile = tempDir.resolve("empty.fvecs");
        Files.createFile(testFile);

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertFalse(it.hasNext());
    }

    private Path createFvecsFile(int numVectors, int dim) throws IOException {
        Path file = tempDir.resolve("test.fvecs");
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {

            for (int i = 0; i < numVectors; i++) {
                dos.writeInt(Integer.reverseBytes(dim));
                for (int j = 0; j < dim; j++) {
                    float val = (float) (i * dim + j);
                    dos.writeInt(Integer.reverseBytes(Float.floatToIntBits(val)));
                }
            }
        }
        return file;
    }
}