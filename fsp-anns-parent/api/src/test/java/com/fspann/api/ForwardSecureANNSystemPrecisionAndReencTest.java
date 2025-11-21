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
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemPrecisionAndReencTest {

    @TempDir
    Path tmp;

    private static final Pattern CSV_SPLIT =
            Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

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

            "output": { "resultsDir": "%s", "exportArtifacts": true, "suppressLegacyMetrics": false },

            "eval":   { "computePrecision": true, "writeGlobalPrecisionCsv": true, "kVariants": [1,5] },

            "ratio":  { "source": "base", "gtSample": 0, "gtMismatchTolerance": 0.05 },

            "lsh":    { "numTables": 8, "rowsPerBand": 1, "probeShards": 32 },

            "paper":  { "enabled": false },
            "reencryption": { "enabled": true },
            "audit":  { "enable": true, "k": 5, "sampleEvery": 1, "worstKeep": 2 }
          }
        """.formatted(resultsDir.replace("\\", "\\\\"));
        Files.writeString(p, json, CREATE, TRUNCATE_EXISTING);
        return p;
    }

    private static Path findUnder(Path root, String filenameStartsWith) throws IOException {
        try (var s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT)
                            .startsWith(filenameStartsWith.toLowerCase(Locale.ROOT)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Couldn't find " + filenameStartsWith + " under " + root));
        }
    }

    private static int colIndex(String[] headerCols, String wanted) {
        for (int i = 0; i < headerCols.length; i++) {
            String name = headerCols[i].trim().replace("\"", "");
            if (name.equalsIgnoreCase(wanted)) return i;
        }
        return -1;
    }

    @Test
    void globalPrecisionCsv_hasK1WithMacroAndMicroEquals1_andReencSummaryExists() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0 (true NN for query)
                {4.0f, 4.0f}
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}  // equals id 0 → precision@1 should be 1.0
        });
        Path gt = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{0});
        Path conf = writeConfig(tmp.resolve("conf.json"), tmp.toString());

        // ensure ratio path has a baseReader to avoid NaN
        System.setProperty("base.path", base.toString());

        // metadata + points + keystore
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

        keySvc.activateVersion(keySvc.getCurrentVersion().getVersion());

        var sys = new ForwardSecureANNSystem(
                conf.toString(), base.toString(), keystore.toString(),
                java.util.List.of(dim), tmp, false, mdm, crypto, 512
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // --- global_precision.csv checks (macro/micro for K=1) ---
        Path globalCsv = findUnder(tmp, "global_precision");
        assertTrue(Files.exists(globalCsv), "global_precision.csv should exist");
        List<String> glines = Files.readAllLines(globalCsv);
        assertTrue(glines.size() >= 2, "global_precision.csv must have header + at least one row");

        String[] h = CSV_SPLIT.split(glines.get(0), -1);
        int kIdx     = colIndex(h, "topK");
        int precisionIdx = colIndex(h, "precision");
        int rrIdx    = colIndex(h, "return_rate");
        assertTrue(kIdx >= 0 && precisionIdx >= 0 && rrIdx >= 0,
                "Missing expected columns in global_precision.csv");

        boolean sawK1 = false;
        for (int i = 1; i < glines.size(); i++) {
            String[] cols = CSV_SPLIT.split(glines.get(i), -1);
            if ("1".equals(cols[kIdx].trim())) {
                double macro = Double.parseDouble(cols[precisionIdx]);
                double rr    = Double.parseDouble(cols[rrIdx]);
                assertEquals(1.0, macro, 1e-6, "precision@1");
                assertEquals(1.0, rr,    1e-6, "return_rate@1");
                sawK1 = true;
                break;
            }
        }
        assertTrue(sawK1, "No row with topK=1 in global_precision.csv");

        // --- reencrypt_metrics.csv end-summary ---
        Path reencCsv = findUnder(tmp, "reencrypt_metrics");
        List<String> rlines = Files.readAllLines(reencCsv);
        assertTrue(rlines.size() >= 2, "reencrypt_metrics.csv needs header + rows");

        // find SUMMARY row
        boolean sawSummary = false;
        for (int i = rlines.size() - 1; i >= 1; i--) {
            String line = rlines.get(i);
            if (line.startsWith("SUMMARY,")) {
                String[] cols = line.split(",", -1);
                // QueryID,TargetVersion,Mode,Touched,TouchedUniqueSoFar,TouchedCumulativeUnique,
                // Reencrypted,AlreadyCurrent,Retried,TimeMs,BytesDelta,BytesAfter
                assertEquals("end", cols[2], "Mode should be 'end' for end-only re-encryption");
                assertTrue(Long.parseLong(cols[3]) >= 0, "Touched should be >= 0");
                assertTrue(Long.parseLong(cols[11]) >= 0, "BytesAfter should be >= 0");
                sawSummary = true;
                break;
            }
        }
        assertTrue(sawSummary, "Did not find SUMMARY row in reencrypt_metrics.csv");
    }
}
