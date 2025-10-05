package com.fspann.api;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.crypto.AesGcmCryptoService;
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

class ForwardSecureANNSystemRatioAutoFallbackTest {

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
        "numShards": 32,
        "opsThreshold": 100000,
        "ageThresholdMs": 86400000,

        "output": { "resultsDir": "%s", "exportArtifacts": false, "suppressLegacyMetrics": true },

        "eval":   { "computePrecision": false, "writeGlobalPrecisionCsv": false, "kVariants": [1,5] },

        "ratio":  { "source": "auto", "gtSample": 1, "gtMismatchTolerance": 0.0 },

        "lsh":    { "numTables": 8, "rowsPerBand": 1, "probeShards": 32 },

        "paper":  { "enabled": false }
      }
    """.formatted(resultsDir.replace("\\", "\\\\"));
        Files.writeString(p, json, CREATE, TRUNCATE_EXISTING);
        return p;
    }

    private static int colIndex(String[] headerCols, String wanted) {
        for (int i = 0; i < headerCols.length; i++) {
            String name = headerCols[i].trim().replace("\"", "");
            if (name.equalsIgnoreCase(wanted)) return i;
        }
        return -1;
    }

    private static int colIndexAny(String[] headerCols, String... names) {
        for (String n : names) {
            int idx = colIndex(headerCols, n);
            if (idx >= 0) return idx;
        }
        return -1;
    }

    private static Path findUnder(Path root, String... namePrefixes) throws IOException {
        try (var s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> {
                        String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
                        for (String pref : namePrefixes) {
                            if (n.startsWith(pref.toLowerCase(Locale.ROOT))) return true;
                        }
                        return false;
                    })
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                            "Could not find file with prefixes " + String.join(", ", namePrefixes) +
                                    " under " + root));
        }
    }

    @Test
    void autoFallback_usesBaseDenom_whenGtIsWrong() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0  (true NN)
                {9.0f, 9.0f}  // id 1
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}  // equals id 0
        });

        // Wrong GT on purpose: says NN is id=1
        Path gt = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{1});
        Path conf = writeConfig(tmp.resolve("conf.json"), tmp.toString());

        Path metaDir = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // Ensure baseReader present and force strict GT trust gate → fallback to base denom
        System.setProperty("base.path", base.toString());
        System.setProperty("ratio.gtMismatchTolerance", "0.0");
        // (Optionally support alt key names without breaking anything)
        System.setProperty("ratio.gt.tolerance", "0.0");

        var mdm = RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        Path keystore = tmp.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());
        var km = new KeyManager(keystore.toString());
        var policy = new KeyRotationPolicy(1_000_000, 7 * 24 * 3_600_000L);
        var keySvc = new KeyRotationServiceImpl(km, policy, metaDir.toString(), mdm, null);
        var crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, mdm);
        keySvc.setCryptoService(crypto);

        keySvc.activateVersion(keySvc.getCurrentVersion().getVersion());

        var sys = new ForwardSecureANNSystem(
                conf.toString(),
                base.toString(),
                keystore.toString(),
                List.of(dim),
                tmp, false, mdm, crypto, 512
        );
        sys.setExitOnShutdown(false);

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // Find results_table*.csv anywhere under results dir
        Path results = findUnder(tmp, "results_table");
        assertTrue(Files.exists(results), "results_table*.csv should exist");

        var lines = Files.readAllLines(results);
        assertFalse(lines.isEmpty());
        String[] header = CSV_SPLIT.split(lines.get(0), -1);

        int idxTopK  = colIndexAny(header, "TopK", "K", "RequestedK");
        int idxRatio = colIndexAny(header, "Ratio", "LiteratureRatio", "AvgRatio", "AverageRatio");
        assertTrue(idxTopK >= 0 && idxRatio >= 0, "Expected CSV columns not found");

        Double ratio = null;
        for (int i = 1; i < lines.size(); i++) {
            String[] cols = CSV_SPLIT.split(lines.get(i), -1);
            if (cols.length <= Math.max(idxTopK, idxRatio)) continue;
            if ("1".equals(cols[idxTopK].trim())) {
                String r = cols[idxRatio].trim();
                ratio = "NaN".equalsIgnoreCase(r) ? Double.NaN : Double.parseDouble(r);
                break;
            }
        }

        assertNotNull(ratio, "Should parse ratio for K=1");
        assertEquals(1.0, ratio, 1e-6, "AUTO should ignore bad GT and use base-scan denominator");

        int idxSrc = colIndexAny(header, "RatioDenomSource", "DenominatorSource");
        assertTrue(idxSrc >= 0, "RatioDenomSource column missing");
        String anyRow = lines.size() > 1 ? lines.get(1) : null;
        assertNotNull(anyRow);
        String[] rc = CSV_SPLIT.split(anyRow, -1);
        String label = rc[idxSrc].trim().toLowerCase(Locale.ROOT);
        // Depending on which writer branch you kept, label may be "base" or "base(auto)"
        assertTrue(label.equals("base") || label.equals("base(auto)"),
                "Expected base denominator under AUTO fallback, got: " + label);

    }
}
