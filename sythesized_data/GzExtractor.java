import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public final class GzExtractor {

    private static final int READ_BUFFER_BYTES = 1 << 20; // 1 MiB read buffer for large files
    private static final int IO_BUFFER_BYTES   = 1 << 20; // 1 MiB for buffered streams

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);

        if (cfg.srcDir == null) {
            System.err.println("Usage: java GzExtractor --src <folder> [--workers N] [--overwrite] [--delete-gz] [--pattern \"**/*.gz\"] [--dry-run] [--quiet]");
            System.exit(2);
        }
        Path root = cfg.srcDir.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            System.err.println("Not a directory: " + root);
            System.exit(2);
        }

        final PathMatcher matcher = root.getFileSystem()
                .getPathMatcher("glob:" + (cfg.pattern == null ? "**/*.gz" : cfg.pattern));

        List<Path> gzFiles = new ArrayList<>(4096);
        System.out.println("Scanning for .gz files under: " + root);
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) {
                    Path rel = root.relativize(file);
                    if (matcher.matches(rel)) {
                        gzFiles.add(file);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        if (gzFiles.isEmpty()) {
            System.out.println("No .gz files matched pattern. Nothing to do.");
            return;
        }

        System.out.printf(Locale.ROOT, "Found %,d file(s). Workers=%d, Overwrite=%s, DeleteGz=%s%n",
                gzFiles.size(), cfg.workers, cfg.overwrite, cfg.deleteGz);

        if (cfg.dryRun) {
            for (Path gz : gzFiles) {
                System.out.println("[dry-run] would extract: " + gz + " -> " + outputPathFor(gz));
            }
            return;
        }

        int workers = Math.max(1, cfg.workers);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CompletionService<Result> ecs = new ExecutorCompletionService<>(pool);

        long submitTs = System.nanoTime();
        for (int i = 0; i < gzFiles.size(); i++) {
            final int idx = i;
            final Path gz = gzFiles.get(i);
            ecs.submit(() -> extractOne(gz, outputPathFor(gz), cfg, idx + 1, gzFiles.size()));
        }

        int done = 0, ok = 0, skipped = 0, failed = 0;
        try {
            while (done < gzFiles.size()) {
                Future<Result> f = ecs.take();
                Result r = f.get();
                done++;
                switch (r.status) {
                    case SKIPPED -> { skipped++; if (!cfg.quiet) System.out.println(r.message); }
                    case OK      -> { ok++;      System.out.println(r.message); }
                    case FAILED  -> { failed++;  System.err.println(r.message); }
                }
            }
        } finally {
            pool.shutdown();
        }

        double seconds = Duration.ofNanos(System.nanoTime() - submitTs).toMillis() / 1000.0;
        System.out.printf(Locale.ROOT,
                "Done. ok=%d, skipped=%d, failed=%d in %.1fs%n", ok, skipped, failed, seconds);

        if (failed > 0) System.exit(1);
    }

    private static Path outputPathFor(Path gzFile) {
        String name = gzFile.getFileName().toString();
        if (name.toLowerCase(Locale.ROOT).endsWith(".gz")) {
            name = name.substring(0, name.length() - 3);
        } else {
            name = name + ".out"; // fallback
        }
        return gzFile.getParent().resolve(name);
    }

    private static Result extractOne(Path gz, Path out, Config cfg, int index, int total) {
        try {
            // sanity
            if (!Files.isRegularFile(gz)) {
                return Result.skip(gz, "Skipping non-regular file: " + gz);
            }
            // skip logic
            if (!cfg.overwrite && Files.exists(out) && Files.size(out) > 0) {
                return Result.skip(gz, progressPrefix(index, total) + "exists -> " + out);
            }

            // extract to temp then move (atomic-ish)
            Path tmp = out.resolveSibling(out.getFileName().toString() + ".partial");
            Files.createDirectories(out.getParent());

            long start = System.nanoTime();
            long written;
            try (InputStream fis = Files.newInputStream(gz, StandardOpenOption.READ);
                 BufferedInputStream bis = new BufferedInputStream(fis, IO_BUFFER_BYTES);
                 GZIPInputStream gzin = new GZIPInputStream(bis, READ_BUFFER_BYTES);
                 OutputStream fos = Files.newOutputStream(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                 BufferedOutputStream bos = new BufferedOutputStream(fos, IO_BUFFER_BYTES)) {

                byte[] buf = new byte[READ_BUFFER_BYTES];
                long w = 0;
                int r;
                while ((r = gzin.read(buf)) != -1) {
                    bos.write(buf, 0, r);
                    w += r;
                }
                bos.flush();
                written = w;
            }

            // move into place
            Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // optionally delete original .gz
            if (cfg.deleteGz) {
                try { Files.deleteIfExists(gz); } catch (IOException ignore) { /* non-fatal */ }
            }

            double sec = Duration.ofNanos(System.nanoTime() - start).toMillis() / 1000.0;
            double mb = written / (1024.0 * 1024.0);
            double mbps = (sec > 0) ? (mb / sec) : 0.0;

            String msg = String.format(Locale.ROOT,
                    "%s%s -> %s  (%,.1f MiB in %.2fs, %.1f MiB/s)",
                    progressPrefix(index, total), gz.getFileName(), out.getFileName(), mb, sec, mbps);

            return Result.ok(gz, msg);
        } catch (Exception e) {
            // best effort cleanup of temp
            try {
                Path tmp = out.resolveSibling(out.getFileName().toString() + ".partial");
                Files.deleteIfExists(tmp);
            } catch (IOException ignore) {}
            return Result.fail(gz, progressPrefix(index, total) + "FAILED " + gz.getFileName() + " : " + e.getMessage());
        }
    }

    private static String progressPrefix(int index, int total) {
        return String.format(Locale.ROOT, "[%d/%d] ", index, total);
    }

    // ---------- small DTOs ----------
    private enum Status { OK, SKIPPED, FAILED }

    private record Result(Status status, Path file, String message) {
        static Result ok(Path f, String m)     { return new Result(Status.OK, f, m); }
        static Result skip(Path f, String m)   { return new Result(Status.SKIPPED, f, m); }
        static Result fail(Path f, String m)   { return new Result(Status.FAILED, f, m); }
    }

    private static final class Config {
        Path srcDir;
        int  workers = Math.min(Runtime.getRuntime().availableProcessors(), 4); // sensible default for big files
        boolean overwrite = false;
        boolean deleteGz  = false;
        boolean dryRun    = false;
        boolean quiet     = false;
        String  pattern   = "**/*.gz";

        static Config parse(String[] args) {
            Config c = new Config();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--src" -> { c.srcDir = Paths.get(args[++i]); }
                    case "--workers" -> { c.workers = Integer.parseInt(args[++i]); }
                    case "--overwrite" -> { c.overwrite = true; }
                    case "--delete-gz" -> { c.deleteGz = true; }
                    case "--dry-run" -> { c.dryRun = true; }
                    case "--quiet" -> { c.quiet = true; }
                    case "--pattern" -> { c.pattern = args[++i]; }
                    default -> {
                        // allow bare src path as first arg
                        if (c.srcDir == null) {
                            c.srcDir = Paths.get(args[i]);
                        } else {
                            System.err.println("Unknown arg: " + args[i]);
                        }
                    }
                }
            }
            return c;
        }
    }
}
