package com.fspann.loader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

@DisplayName("GroundtruthManager Unit Tests")
public class GroundtruthManagerTest {

    private GroundtruthManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        manager = new GroundtruthManager();
    }

    @Test
    @DisplayName("Test GroundtruthManager tracks loaded rows")
    public void testTrackLoadedRows() throws Exception {
        Path gtFile = createIvecsFile(10, 100);
        manager.load(gtFile.toString());

        assertEquals(10, manager.size());
    }

    @Test
    @DisplayName("Test GroundtruthManager returns full groundtruth")
    public void testGetFullGroundtruth() throws Exception {
        Path gtFile = createIvecsFile(5, 50);
        manager.load(gtFile.toString());

        int[] row = manager.getGroundtruth(0);
        assertEquals(50, row.length);
    }

    @Test
    @DisplayName("Test GroundtruthManager returns top-K")
    public void testGetTopK() throws Exception {
        Path gtFile = createIvecsFile(5, 100);
        manager.load(gtFile.toString());

        int[] topK = manager.getGroundtruth(0, 10);
        assertEquals(10, topK.length);
    }

    @Test
    @DisplayName("Test GroundtruthManager validates dataset consistency")
    public void testConsistencyCheck() throws Exception {
        Path gtFile = createIvecsFile(5, 100);
        manager.load(gtFile.toString());

        assertTrue(manager.isConsistentWithDatasetSize(1000));
        assertFalse(manager.isConsistentWithDatasetSize(10));
    }

    private Path createIvecsFile(int numRows, int dim) throws IOException {
        Path file = tempDir.resolve("groundtruth.ivecs");
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {

            for (int i = 0; i < numRows; i++) {
                dos.writeInt(Integer.reverseBytes(dim));
                for (int j = 0; j < dim; j++) {
                    dos.writeInt(Integer.reverseBytes(j % 100));
                }
            }
        }
        return file;
    }
}