package com.fspann.loader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;

@DisplayName("DefaultDataLoader Unit Tests")
public class DefaultDataLoaderTest {

    private DefaultDataLoader loader;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        loader = new DefaultDataLoader();
    }

    @Test
    @DisplayName("Test DefaultDataLoader recognizes FVECS format")
    public void testRecognizesFvecs() throws Exception {
        Path file = tempDir.resolve("test.fvecs");
        Files.createFile(file);

        FormatLoader fmt = loader.lookup(file);
        assertNotNull(fmt);
        assertTrue(fmt instanceof FvecsLoader);
    }

    @Test
    @DisplayName("Test DefaultDataLoader recognizes IVECS format")
    public void testRecognizesIvecs() throws Exception {
        Path file = tempDir.resolve("test.ivecs");
        Files.createFile(file);

        FormatLoader fmt = loader.lookup(file);
        assertNotNull(fmt);
        assertTrue(fmt instanceof IvecsLoader);
    }

    @Test
    @DisplayName("Test DefaultDataLoader recognizes BVECS format")
    public void testRecognizesBvecs() throws Exception {
        Path file = tempDir.resolve("test.bvecs");
        Files.createFile(file);

        FormatLoader fmt = loader.lookup(file);
        assertNotNull(fmt);
        assertTrue(fmt instanceof BvecsLoader);
    }

    @Test
    @DisplayName("Test DefaultDataLoader handles unsupported format")
    public void testUnsupportedFormat() throws Exception {
        Path file = tempDir.resolve("test.xyz");
        Files.createFile(file);

        assertThrows(IllegalArgumentException.class, () -> loader.lookup(file));
    }

    @Test
    @DisplayName("Test DefaultDataLoader closes properly")
    public void testClose() throws Exception {
        loader.close();
        // Should not throw
    }
}