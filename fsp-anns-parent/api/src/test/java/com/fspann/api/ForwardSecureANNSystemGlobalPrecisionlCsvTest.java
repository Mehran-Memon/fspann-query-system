package com.fspann.api;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemGlobalPrecisionCsvTest {

    @TempDir
    Path tmp;

    private static Path writeFvecs(Path p, int dim, float[][] rows) throws IOException {
        try (FileChannel ch = FileChannel.open(p, CREATE, TRUNCATE_EXISTING, WRITE)) {
            ByteBuffer rec = ByteBuffer.allocate(4 + 4 * dim).order(ByteOrder.LITTLE_ENDIAN);
            for (float[] r : rows) {
                rec.clear();
                rec.putInt(dim);
                for (float v : r) rec.putFloat(v);
                rec.flip();
                ch.write(rec);
            }
        }
        return p;
    }

    private static Path writeIvecs(Path p, int[] row) throws IOException {
        try (FileChannel ch = FileChannel.open(p, CREATE, TRUNCATE_EXISTING, WRITE)) {
            ByteBuffer bb = ByteBuffer.allocate(4 + 4 * row.length).order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(row.length);
            for (int v : row) bb.putInt(v);
            bb.flip();
            ch.write(bb);
        }
        return p;
    }

    private static Path writeConfig(Path p, String resultsDir) throws IOException {
        String json = """
          {
            "numShards": 32, "numTables": 4,
            "opsThreshold": 100000, "ageThresholdMs": 86400000,
            "reEncBatchSize": 64,
            "profilerEnabled": true,
            "eval": { "computePrecision": true, "writeGlobalPrecisionCsv": true },
            "output": { "resultsDir": "%s" }
          }
        """.formatted(resultsDir.replace("\\", "\\\\"));
        Files.writeString(p, json, CREATE, TRUNCATE_EXISTING);
        return p;
    }

    @Test
    void writesGlobalPrecisionCsv_andPrecisionIsPresent() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0
                {2.0f, 2.0f}  // id 1
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}  // NN = id 0
        });
        Path gt    = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{0}); // correct GT
        Path conf  = writeConfig(tmp.resolve("conf.json"), tmp.toString());

        Path metaDir   = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        System.setProperty("base.path", base.toString());

        RocksDBMetadataManager mdm =
                RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        Path keystore = tmp.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());
        KeyManager km = new KeyManager(keystore.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(1_000_000, 7 * 24 * 3600_000L);
        KeyRotationServiceImpl keySvc =
                new KeyRotationServiceImpl(km, policy, metaDir.toString(), mdm, null);
        var crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, mdm);
        keySvc.setCryptoService(crypto);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                conf.toString(),
                base.toString(),
                keystore.toString(),
                List.of(dim),
                tmp, false, mdm, crypto, 128
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // global_precision.csv should exist and have a precision header
        Path gprec = tmp.resolve("global_precision.csv");
        assertTrue(Files.exists(gprec), "global_precision.csv must be written when enabled");
        var lines = Files.readAllLines(gprec);
        assertFalse(lines.isEmpty(), "global_precision.csv should not be empty");
        assertTrue(lines.get(0).startsWith("dimension,topK,global_precision"),
                "CSV header should start with dimension,topK,global_precision");

        // results_table.csv should exist and include a "Precision" column in the header
        Path results = tmp.resolve("results_table.csv");
        assertTrue(Files.exists(results), "results_table.csv should exist");
        String header = Files.readAllLines(results).get(0);
        assertTrue(header.toLowerCase().contains("precision"),
                "Precision column should exist in results table header");
    }
}
