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
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;
import java.util.Locale;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemResultsTableSchemaTest {

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
                "numShards": 8,
                "opsThreshold": 100000,
                "ageThresholdMs": 86400000,
                "output": { "resultsDir": "%s" },
                "eval":   { "computePrecision": true, "writeGlobalPrecisionCsv": true, "kVariants": [1] },
                "ratio":  { "source": "auto", "gtSample": 0 },
                "lsh":    { "numTables": 4, "rowsPerBand": 2, "probeShards": 8 },
                "paper":  { "enabled": false },
                "reencryption": { "enabled": true }
              }
            """.formatted(resultsDir.replace("\\", "\\\\"));
        Files.writeString(p, json, CREATE, TRUNCATE_EXISTING);
        return p;
    }

    private static Path findUnder(Path root, String prefix) throws IOException {
        try (var s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).startsWith(prefix))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Could not find " + prefix + " under " + root));
        }
    }

    @Test
    void resultsTable_hasReturned_and_RatioDenomSource_columns() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, {2.0f, 2.0f}, {3.0f, 3.0f}
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}
        });
        Path gt = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{0});
        Path conf = writeConfig(tmp.resolve("conf.json"), tmp.toString());

        System.setProperty("base.path", base.toString());

        Path metaDir = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);
        RocksDBMetadataManager mdm =
                RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        Path keystore = tmp.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());
        var km = new KeyManager(keystore.toString());
        var policy = new KeyRotationPolicy(1_000_000, 7L * 24 * 3_600_000L);
        var keySvc = new KeyRotationServiceImpl(km, policy, metaDir.toString(), mdm, null);
        var crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, mdm);
        keySvc.setCryptoService(crypto);

        var sys = new ForwardSecureANNSystem(
                conf.toString(), base.toString(), keystore.toString(),
                java.util.List.of(dim), tmp, false, mdm, crypto, 256
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        Path results = findUnder(tmp, "results_table");
        List<String> lines = Files.readAllLines(results);
        assertFalse(lines.isEmpty());
        String header = lines.get(0);

        // schema bits we expect after the rename and label additions
        assertTrue(header.contains("TopK"), "Header should contain TopK");
        assertTrue(header.contains("Returned"), "Header should contain 'Returned' (renamed from Retrieved)");
        assertTrue(header.contains("Ratio"), "Header should contain 'Ratio'");
        assertTrue(header.contains("Precision"), "Header should contain 'Precision'");
        assertTrue(header.contains("RatioDenomSource"), "Header should contain 'RatioDenomSource'");
        assertTrue(header.contains("ScannedCandidates"), "Header should contain 'ScannedCandidates'");
    }
}
