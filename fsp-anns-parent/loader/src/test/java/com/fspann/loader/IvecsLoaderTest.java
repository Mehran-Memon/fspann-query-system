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

@DisplayName("IVECS Loader Unit Tests")
public class IvecsLoaderTest {

    private IvecsLoader loader;
    private Path testFile;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        loader = new IvecsLoader();
    }

    @Test
    @DisplayName("Test IVECS loader reads single index row")
    public void testReadSingleRow() throws Exception {
        testFile = createIvecsFile(1, 100);

        Iterator<int[]> it = loader.openIndexIterator(testFile);
        assertTrue(it.hasNext());

        int[] row = it.next();
        assertNotNull(row);
        assertEquals(100, row.length);
        assertFalse(it.hasNext());
    }

    @Test
    @DisplayName("Test IVECS loader reads multiple rows")
    public void testReadMultipleRows() throws Exception {
        testFile = createIvecsFile(5, 100);

        Iterator<int[]> it = loader.openIndexIterator(testFile);
        int count = 0;
        while (it.hasNext()) {
            int[] row = it.next();
            assertEquals(100, row.length);
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    @DisplayName("Test IVECS loader preserves integer values")
    public void testIntegerValuesPreserved() throws Exception {
        testFile = tempDir.resolve("test.ivecs");
        int[] expected = {100, 200, 300, 400};
        int dim = expected.length;

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(testFile)))) {

            dos.writeInt(Integer.reverseBytes(dim));
            for (int v : expected) {
                dos.writeInt(Integer.reverseBytes(v));
            }
        }

        Iterator<int[]> it = loader.openIndexIterator(testFile);
        int[] read = it.next();

        assertArrayEquals(expected, read);
    }

    @Test
    @DisplayName("Test IVECS vector iterator throws UnsupportedOperationException")
    public void testVectorIteratorUnsupported() throws Exception {
        testFile = createIvecsFile(1, 100);
        assertThrows(UnsupportedOperationException.class,
                () -> loader.openVectorIterator(testFile));
    }

    @Test
    @DisplayName("Test IVECS loader rejects invalid dimension")
    public void testInvalidDimension() throws Exception {
        testFile = tempDir.resolve("invalid.ivecs");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(Integer.reverseBytes(2_000_000));
        }

        assertThrows(UncheckedIOException.class, () -> loader.openIndexIterator(testFile));
    }

    private Path createIvecsFile(int numRows, int dim) throws IOException {
        Path file = tempDir.resolve("test.ivecs");
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {

            for (int i = 0; i < numRows; i++) {
                dos.writeInt(Integer.reverseBytes(dim));
                for (int j = 0; j < dim; j++) {
                    dos.writeInt(Integer.reverseBytes(i * dim + j));
                }
            }
        }
        return file;
    }
}