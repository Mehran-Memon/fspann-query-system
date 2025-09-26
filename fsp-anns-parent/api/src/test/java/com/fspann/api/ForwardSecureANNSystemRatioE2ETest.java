package com.fspann.api;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.crypto.AesGcmCryptoService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
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

class ForwardSecureANNSystemRatioE2ETest {

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
                    "numShards": 32, "numTables": 4,
                    "opsThreshold": 100000, "ageThresholdMs": 86400000,
                    "reEncBatchSize": 64,
                    "output": { "resultsDir": "%s" }
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
    void ratioAt1_isExactlyOne_whenTrueNNRetrieved() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0 (true NN)
                {4.0f, 4.0f}  // id 1
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}  // equals id 0
        });
        Path gt = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{0}); // top-1 truth is id 0
        Path conf = writeConfig(tmp.resolve("conf.json"), tmp.toString());

        Path metaDir = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // ensure ratio gets computed
        System.setProperty("base.path", base.toString());

        RocksDBMetadataManager mdm =
                RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        Path keystore = tmp.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());
        var km = new KeyManager(keystore.toString());
        var policy = new KeyRotationPolicy(1_000_000, 7 * 24 * 3_600_000L);
        var keySvc = new KeyRotationServiceImpl(km, policy, metaDir.toString(), mdm, null);
        var crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, mdm);
        keySvc.setCryptoService(crypto);

        var sys = new ForwardSecureANNSystem(
                conf.toString(),
                /*dataPath*/ base.toString(),
                /*keysFile*/ keystore.toString(),
                List.of(dim),
                tmp, /*verbose*/ false, mdm, crypto, /*batchSize*/ 1024
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // results_table*.csv may be written under subfolders; find it
        Path results = findUnder(tmp, "results_table");
        assertTrue(Files.exists(results), "results_table*.csv should exist");
        List<String> lines = Files.readAllLines(results);
        assertFalse(lines.isEmpty(), "CSV is empty");

        String[] header = CSV_SPLIT.split(lines.get(0), -1);
        int idxQ     = colIndexAny(header, "qIndex", "queryIndex", "query");
        int idxTopK  = colIndexAny(header, "TopK", "K", "RequestedK");
        int idxRatio = colIndexAny(header, "Ratio", "LiteratureRatio", "AvgRatio", "AverageRatio");
        assertTrue(idxQ >= 0 && idxTopK >= 0 && idxRatio >= 0, "Expected CSV columns not found");

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
        assertNotNull(ratio, "No row for K=1 in results_table.csv");
        assertEquals(1.0, ratio, 1e-6);
    }

    @Test
    void averageRatio_matchesCsvAndMetricsSummary_forTrivialDataset() throws Exception {
        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0 (true NN)
                {4.0f, 4.0f}  // id 1
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

        var mdm = RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        Path keystore = tmp.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());
        var km = new KeyManager(keystore.toString());
        var policy = new KeyRotationPolicy(1_000_000, 7 * 24 * 3_600_000L);
        var keySvc = new KeyRotationServiceImpl(km, policy, metaDir.toString(), mdm, null);
        var crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, mdm);
        keySvc.setCryptoService(crypto);

        var sys = new ForwardSecureANNSystem(
                conf.toString(),
                base.toString(),
                keystore.toString(),
                List.of(dim),
                tmp,
                false,
                mdm,
                crypto,
                1024
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.exportArtifacts(tmp); // writes metrics_summary*.txt
        sys.shutdown();

        // CSV has Ratio==1.0 (or Literature/Avg variants) at K=1
        Path csv = findUnder(tmp, "results_table");
        assertTrue(Files.exists(csv), "results_table*.csv should exist");
        List<String> lines = Files.readAllLines(csv);
        String[] header = CSV_SPLIT.split(lines.get(0), -1);
        int idxTopK  = colIndexAny(header, "TopK", "K", "RequestedK");
        int idxRatio = colIndexAny(header, "Ratio", "LiteratureRatio", "AvgRatio", "AverageRatio");
        assertTrue(idxTopK >= 0 && idxRatio >= 0, "Expected CSV columns not found");

        Double k1Ratio = null;
        for (int i = 1; i < lines.size(); i++) {
            String[] cols = CSV_SPLIT.split(lines.get(i), -1);
            if (cols.length <= Math.max(idxTopK, idxRatio)) continue;
            if ("1".equals(cols[idxTopK].trim())) {
                String r = cols[idxRatio].trim();
                k1Ratio = "NaN".equalsIgnoreCase(r) ? Double.NaN : Double.parseDouble(r);
                break;
            }
        }
        assertNotNull(k1Ratio, "Could not find K=1 row in CSV");
        assertEquals(1.0, k1Ratio, 1e-6);

        // metrics_summary*.txt AvgRatio == 1.000000 (accept AvgRatio or AverageRatio, any spacing)
        Path summary = findUnder(tmp, "metrics_summary", "query_metrics_summary");
        assertTrue(Files.exists(summary), "metrics_summary*.txt should exist");
        String summaryText = Files.readString(summary);

        var m = java.util.regex.Pattern
                .compile("(AvgRatio|AverageRatio)\\s*=\\s*([+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?)",
                        java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(summaryText);
        assertTrue(m.find(), "AvgRatio=... not found in metrics summary");
        double avgRatio = Double.parseDouble(m.group(2));
        assertEquals(1.0, avgRatio, 1e-6);
    }
}
