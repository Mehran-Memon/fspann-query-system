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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemRatioAutoFallbackTest {

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
    void autoFallback_usesBaseDenom_whenGtIsWrong() throws Exception {
        Files.createDirectories(Paths.get("results"));
        Files.deleteIfExists(Paths.get("results", "results_table.txt"));

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
        Path conf = writeConfig(tmp.resolve("conf.json"));

        Path metaDir = tmp.resolve("meta");
        Path pointsDir = tmp.resolve("points");
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // Ensure baseReader is present and AUTO is in effect (default)
        System.setProperty("base.path", base.toString());
        // Make trust gate stricter (any mismatch fails)
        System.setProperty("ratio.gtMismatchTolerance", "0.0");

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
                tmp, false, mdm, crypto, 512
        );

        sys.runEndToEnd(base.toString(), query.toString(), dim, gt.toString());
        sys.shutdown();

        // Parse K=1 LiteratureRatio from results_table.txt; with AUTO fallback it should be 1.0
        Path results = Paths.get("results", "results_table.txt");
        assertTrue(Files.exists(results), "results_table.txt should exist");

        var lines = Files.readAllLines(results);

        var norm = (java.util.function.Function<String,String>) s -> s
                .replaceAll("[\\u2500-\\u257F]", " ")
                .replace('|', ' ')
                .replace('+', ' ');

        Double ratio = null;
        Pattern num = Pattern.compile("(NaN|[-+]?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?)");

        boolean inTable = false;
        for (String raw : lines) {
            String s = norm.apply(raw).trim();
            if (s.isEmpty()) continue;
            if (!inTable && s.toLowerCase(Locale.ROOT).contains("literatureratio")) {
                inTable = true; continue;
            }
            if (!inTable) continue;
            if (s.matches("^[^\\p{Alnum}]+$")) continue;

            List<String> toks = new ArrayList<>();
            Matcher m = num.matcher(s);
            while (m.find()) toks.add(m.group(1));
            if (toks.size() < 3) continue;

            double kVal;
            try { kVal = Double.parseDouble(toks.get(0)); } catch (NumberFormatException ex) { continue; }
            if (Math.rint(kVal) != 1.0) continue;

            String ratioStr = toks.get(2);
            ratio = "NaN".equalsIgnoreCase(ratioStr) ? Double.NaN : Double.parseDouble(ratioStr);
            break;
        }

        assertNotNull(ratio, "Should parse ratio for K=1");
        assertEquals(1.0, ratio, 1e-6, "AUTO should ignore bad GT and use base-scan denominator");
    }
}
