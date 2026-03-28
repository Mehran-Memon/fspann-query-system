package com.fspann.api;

import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.loader.GroundtruthManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ForwardSecureANNSystemTest {

    private ForwardSecureANNSystem system;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize the ForwardSecureANNSystem with mock or real configurations
        Path configPath = Paths.get("path/to/config.json");
        Path metadataPath = Paths.get("path/to/metadata");
        Path keysFilePath = Paths.get("path/to/keys");
        List<Integer> dimensions = List.of(128); // Example dimension

        system = new ForwardSecureANNSystem(
                configPath.toString(),
                "path/to/data",
                keysFilePath.toString(),
                dimensions,
                metadataPath,
                true,
                null,
                new AesGcmCryptoService(new SimpleMeterRegistry(), null, null),
                100000
        );
    }

    @Test
    void testFullPipeline() throws Exception {
        // Use reflection to call the private method
        Method method = ForwardSecureANNSystem.class.getDeclaredMethod("loadQueriesWithValidation", Path.class, int.class);
        method.setAccessible(true);
        List<double[]> queries = (List<double[]>) method.invoke(system, Paths.get("path/to/queries"), 128);

        // Indexing phase
        system.indexStream("path/to/data", 128);

        // Querying phase
        GroundtruthManager gt = new GroundtruthManager();
        gt.load("path/to/groundtruth");
        system.runQueries(queries, 128, gt, true);

        // Assertions
        assertTrue(system.getQueryCache().size() > 0); // Ensure that queries were processed
        assertTrue(system.getIndexedVectorCount() > 0); // Ensure that vectors were indexed
    }

}

