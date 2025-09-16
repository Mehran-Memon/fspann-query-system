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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemRatioE2ETest {

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
        // one row: [dim][row...], little-endian
        try (FileChannel ch = FileChannel.open(p, CREATE, TRUNCATE_EXISTING, WRITE)) {
            ByteBuffer bb = ByteBuffer.allocate(4 + 4 * row.length).order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(row.length);
            for (int v : row) bb.putInt(v);
            bb.flip();
            ch.write(bb);
        }
        return p;
    }

    private static Path writeConfig(Path p) throws IOException {
        String json = """
                  {
                    "numShards": 32, "numTables": 4,
                    "opsThreshold": 100000, "ageThresholdMs": 86400000,
                    "reEncBatchSize": 64
                  }
                """;
        Files.writeString(p, json, CREATE, TRUNCATE_EXISTING);
        return p;
    }

    @Test
    void ratioAt1_isExactlyOne_whenTrueNNRetrieved() throws Exception {
        Files.createDirectories(Paths.get("results"));
        Files.deleteIfExists(Paths.get("results", "results_table.txt"));

        int dim = 2;

        Path base = writeFvecs(tmp.resolve("base.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}, // id 0 (true NN)
                {4.0f, 4.0f}  // id 1
        });
        Path query = writeFvecs(tmp.resolve("query.fvecs"), dim, new float[][]{
                {1.0f, 1.0f}  // equals id 0
        });
        Path gt = writeIvecs(tmp.resolve("gt.ivecs"), new int[]{0}); // top-1 truth is id 0
        Path conf = writeConfig(tmp.resolve("conf.json"));

        Path metaDir = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // surefire runs from module dir; ensure ratio gets computed
        System.setProperty("base.path", base.toString());
        System.setProperty("results.dir", tmp.toString()); // for profiler exports (optional)
        System.setProperty("paper.mode", "false");         // simplest path for tiny test

        // Build required services (same shape as your main())
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
                /*dataPath*/ base.toString(),
                /*keysFile*/ keystore.toString(),
                List.of(dim),
                tmp, /*verbose*/ false, mdm, crypto, /*batchSize*/ 1024
        );

        // run full pipeline: index → query with K variants → results file
        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // --- parse results_table.txt robustly (handles box-drawing and space-aligned tables) ---
        Path results = Paths.get("results", "results_table.txt");
        assertTrue(Files.exists(results), "results_table.txt should exist");
        List<String> lines = Files.readAllLines(results);

// Normalize (convert any box-drawing chars to ASCII) and build a helper
        java.util.function.Function<String,String> norm = s -> s
                .replaceAll("[\\u2500-\\u257F]", " ")   // all box drawing → space
                .replace('|', ' ')                      // pipes → space
                .replace('+', ' ');                     // plus borders → space

// 1) locate the "Query 1 Results" section (don’t depend on dim number)
        int start = -1;
        for (int li = 0; li < lines.size(); li++) {
            if (lines.get(li).contains("Query 1 Results")) { start = li; break; }
        }
        int i = (start >= 0) ? start + 1 : 0;
        boolean inTable = false;

        Double ratio = null;
        Pattern numToken = Pattern.compile("(NaN|[-+]?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?)");

        for (; i < lines.size(); i++) {
            String raw = lines.get(i);
            String s = norm.apply(raw).trim();
            if (s.isEmpty()) continue;

            // End current table if next section begins
            if (s.startsWith("Query ") && start >= 0) break;

            // Detect header row to enter the table
            if (!inTable && s.toLowerCase(Locale.ROOT).contains("topk")
                    && s.toLowerCase(Locale.ROOT).contains("literatureratio")) {
                inTable = true;
                continue;
            }
            if (!inTable) continue;

            // Skip obvious separator rows (all non-alphanumerics/spaces)
            if (s.matches("^[^\\p{Alnum}]+$")) continue;

            // Pull out number tokens from the row
            List<String> tokens = new ArrayList<>();
            Matcher m = numToken.matcher(s);
            while (m.find()) tokens.add(m.group(1));
            if (tokens.size() < 3) continue;

            // Column order: TopK, Retrieved, LiteratureRatio, ...
            double kVal;
            try { kVal = Double.parseDouble(tokens.get(0)); } catch (NumberFormatException ex) { continue; }
            if (Math.rint(kVal) != 1.0) continue;  // we want the K=1 row

            String ratioStr = tokens.get(2);
            if ("NaN".equalsIgnoreCase(ratioStr)) {
                ratio = Double.NaN;
            } else {
                ratio = Double.parseDouble(ratioStr);
            }
            break;
        }

// Fallback: if we never saw a header, scan whole file for the first data row where TopK==1
        if (ratio == null) {
            for (String raw : lines) {
                String s = norm.apply(raw).trim();
                if (s.isEmpty() || s.toLowerCase(Locale.ROOT).contains("topk")) continue;
                if (s.startsWith("Query ")) continue;
                if (s.matches("^[^\\p{Alnum}]+$")) continue;

                List<String> tokens = new ArrayList<>();
                Matcher m = numToken.matcher(s);
                while (m.find()) tokens.add(m.group(1));
                if (tokens.size() < 3) continue;

                double kVal;
                try { kVal = Double.parseDouble(tokens.get(0)); } catch (NumberFormatException ex) { continue; }
                if (Math.rint(kVal) != 1.0) continue;

                String ratioStr = tokens.get(2);
                ratio = "NaN".equalsIgnoreCase(ratioStr) ? Double.NaN : Double.parseDouble(ratioStr);
                break;
            }
        }

        assertNotNull(ratio, "No row for K=1 in our section of results_table.txt");
        assertEquals(1.0, ratio, 1e-6);
    }
}
