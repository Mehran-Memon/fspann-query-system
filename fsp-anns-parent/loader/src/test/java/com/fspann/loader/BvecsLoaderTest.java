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

@DisplayName("BVECS Loader Unit Tests")
public class BvecsLoaderTest {

    private BvecsLoader loader;
    private Path testFile;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        loader = new BvecsLoader();
    }

    @Test
    @DisplayName("Test BVECS loader reads single vector")
    public void testReadSingleVector() throws Exception {
        testFile = createBvecsFile(1, 128);

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        assertTrue(it.hasNext());

        double[] v = it.next();
        assertNotNull(v);
        assertEquals(128, v.length);
        assertFalse(it.hasNext());
    }

    @Test
    @DisplayName("Test BVECS loader reads multiple vectors")
    public void testReadMultipleVectors() throws Exception {
        testFile = createBvecsFile(10, 128);

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
    @DisplayName("Test BVECS loader preserves byte values (0-255)")
    public void testByteValuesPreserved() throws Exception {
        testFile = tempDir.resolve("test.bvecs");
        byte[] expected = {0, 50, 100, (byte)150, (byte)200, (byte)255};

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(testFile)))) {

            int dim = expected.length;

            dos.writeInt(Integer.reverseBytes(dim));
            for (byte b : expected) {
                dos.writeByte(b);
            }
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        double[] read = it.next();

        for (int i = 0; i < expected.length; i++) {
            assertEquals((int)(expected[i] & 0xFF), (int)read[i]);
        }
    }

    @Test
    @DisplayName("Test BVECS loader handles all byte values")
    public void testAllByteValuesSupported() throws Exception {
        testFile = tempDir.resolve("test.bvecs");
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(testFile)))) {

            dos.writeInt(Integer.reverseBytes(256));
            for (int i = 0; i < 256; i++) {
                dos.writeByte((byte) i);
            }
        }

        Iterator<double[]> it = loader.openVectorIterator(testFile);
        double[] read = it.next();

        for (int i = 0; i < 256; i++) {
            assertEquals(i, (int)read[i]);
        }
    }

    @Test
    @DisplayName("Test BVECS index iterator throws UnsupportedOperationException")
    public void testIndexIteratorUnsupported() throws Exception {
        testFile = createBvecsFile(1, 128);
        assertThrows(UnsupportedOperationException.class,
                () -> loader.openIndexIterator(testFile));
    }

    private Path createBvecsFile(int numVectors, int dim) throws IOException {
        Path file = tempDir.resolve("test.bvecs");
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {

            for (int i = 0; i < numVectors; i++) {
                dos.writeInt(Integer.reverseBytes(dim));
                for (int j = 0; j < dim; j++) {
                    dos.writeByte((byte) ((i * dim + j) % 256));
                }
            }
        }
        return file;
    }
}