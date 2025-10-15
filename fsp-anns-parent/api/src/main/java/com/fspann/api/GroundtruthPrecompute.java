package com.fspann.api;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.*;

/** Offline exact ground-truth precomputation for fvecs/bvecs → ivecs. */
public final class GroundtruthPrecompute {

    private static final class VecReader implements AutoCloseable {
        final FileChannel ch;
        final MappedByteBuffer map; // null if streaming
        final boolean bvecs;
        final int dim;
        final int recordBytes;
        final int count;
        final boolean streaming;
        final ThreadLocal<ByteBuffer> tlBuf;

        static VecReader open(Path path, boolean isBvecs, Integer expectDim) throws IOException {
            FileChannel ch = FileChannel.open(path, READ);
            long size = ch.size();

            int dim;
            if (expectDim == null) {
                ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                int n = ch.read(bb, 0);
                if (n != 4) throw new IOException("Cannot read dim from " + path);
                bb.flip();
                dim = bb.getInt();
            } else {
                dim = expectDim;
            }

            int payload = isBvecs ? dim : dim * 4;
            int recBytes = 4 + payload;
            int cnt = (int) (size / recBytes);

            if (size <= Integer.MAX_VALUE) {
                MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_ONLY, 0, size).load();
                map.order(ByteOrder.LITTLE_ENDIAN);
                return new VecReader(ch, map, isBvecs, dim, recBytes, cnt, false);
            } else {
                return new VecReader(ch, null, isBvecs, dim, recBytes, cnt, true);
            }
        }

        private VecReader(FileChannel ch, MappedByteBuffer map, boolean bvecs, int dim,
                          int recBytes, int cnt, boolean streaming) {
            this.ch = ch;
            this.map = map;
            this.bvecs = bvecs;
            this.dim = dim;
            this.recordBytes = recBytes;
            this.count = cnt;
            this.streaming = streaming;
            this.tlBuf = streaming
                    ? ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(recordBytes).order(ByteOrder.LITTLE_ENDIAN))
                    : null;
        }

        /** Read vector i into target[] (float32). */
        void read(int id, float[] target) {
            final int off = id * recordBytes + 4;
            if (!streaming) {
                if (bvecs) {
                    for (int i = 0; i < dim; i++) target[i] = (map.get(off + i) & 0xFF);
                } else {
                    for (int i = 0; i < dim; i++) target[i] = map.getFloat(off + i * 4);
                }
            } else {
                try {
                    ByteBuffer buf = tlBuf.get();
                    buf.clear();
                    int n = ch.read(buf, (long) id * recordBytes);
                    if (n < recordBytes) throw new RuntimeException("Short read");
                    buf.flip();
                    buf.getInt(); // skip dim
                    if (bvecs) {
                        for (int i = 0; i < dim; i++) target[i] = (buf.get() & 0xFF);
                    } else {
                        for (int i = 0; i < dim; i++) target[i] = buf.getFloat();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /** Exact L2 distance between q and base[id]. */
        double l2(float[] q, int id) {
            final int off = id * recordBytes + 4;
            double sum = 0.0;
            if (!streaming) {
                if (bvecs) {
                    for (int i = 0; i < dim; i++) {
                        int ui = map.get(off + i) & 0xFF;
                        double d = q[i] - ui;
                        sum += d * d;
                    }
                } else {
                    for (int i = 0; i < dim; i++) {
                        float v = map.getFloat(off + i * 4);
                        double d = q[i] - v;
                        sum += d * d;
                    }
                }
            } else {
                try {
                    ByteBuffer buf = tlBuf.get();
                    buf.clear();
                    int n = ch.read(buf, (long) id * recordBytes);
                    if (n < recordBytes) throw new IOException("Short read id=" + id);
                    buf.flip();
                    buf.getInt(); // dim
                    if (bvecs) {
                        for (int i = 0; i < dim; i++) {
                            int ui = buf.get() & 0xFF;
                            double d = q[i] - ui;
                            sum += d * d;
                        }
                    } else {
                        for (int i = 0; i < dim; i++) {
                            float v = buf.getFloat();
                            double d = q[i] - v;
                            sum += d * d;
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return Math.sqrt(sum);
        }

        @Override public void close() throws IOException { ch.close(); }
    }

    private static final class HeapK {
        final int k;
        final PriorityQueue<Item> pq = new PriorityQueue<>(Comparator.<Item>comparingDouble(it -> it.d).reversed());
        HeapK(int k) { this.k = k; }
        void offer(int id, double d) {
            if (pq.size() < k) { pq.offer(new Item(id, d)); return; }
            if (!pq.isEmpty() && d < pq.peek().d) { pq.poll(); pq.offer(new Item(id, d)); }
        }
        int[] idsAscending() {
            ArrayList<Item> list = new ArrayList<>(pq);
            list.sort(Comparator.comparingDouble(it -> it.d));
            int[] out = new int[list.size()];
            for (int i = 0; i < out.length; i++) out[i] = list.get(i).id;
            return out;
        }
    }
    private static final class Item { final int id; final double d; Item(int id, double d){this.id=id; this.d=d;} }

    private static boolean looksLike(Path p, String suffix) {
        String s = p.getFileName().toString().toLowerCase(Locale.ROOT);
        return s.endsWith(suffix);
    }

    private static Path defaultOut(Path queryPath) {
        String name = queryPath.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String base = (dot >= 0 ? name.substring(0, dot) : name);
        return queryPath.getParent().resolve(base + "_groundtruth.ivecs");
    }

    private static void writeIvecs(Path out, int[][] rows) throws IOException {
        try (OutputStream raw = Files.newOutputStream(out, CREATE, TRUNCATE_EXISTING, WRITE);
             BufferedOutputStream bos = new BufferedOutputStream(raw, 1<<20)) {
            for (int[] ids : rows) {
                ByteBuffer bb = ByteBuffer.allocate(4 + 4 * ids.length).order(ByteOrder.LITTLE_ENDIAN);
                bb.putInt(ids.length);
                for (int id : ids) bb.putInt(id);
                bb.flip();
                bos.write(bb.array(), 0, bb.remaining());
            }
        }
    }

    /** Run precompute. base & query must both be .fvecs or both .bvecs. Returns the out path. */
    public static Path run(Path basePath, Path queryPath, Path outPath, int K, int threads) throws Exception {
        boolean baseIsB = looksLike(basePath, ".bvecs");
        boolean qryIsB  = looksLike(queryPath, ".bvecs");
        boolean baseIsF = looksLike(basePath, ".fvecs");
        boolean qryIsF  = looksLike(queryPath, ".fvecs");
        if (!(baseIsB || baseIsF) || !(qryIsB || qryIsF)) {
            throw new IllegalArgumentException("Files must be .fvecs or .bvecs");
        }
        if (baseIsB != qryIsB) {
            throw new IllegalArgumentException("Base and query types must match (both fvecs or both bvecs)");
        }
        Files.createDirectories(outPath.toAbsolutePath().getParent());

        long t0 = System.currentTimeMillis();
        try (VecReader base = VecReader.open(basePath, baseIsB, null);
             VecReader qry  = VecReader.open(queryPath, qryIsB, base.dim)) {

            if (base.dim != qry.dim) {
                throw new IllegalArgumentException("Dim mismatch base=" + base.dim + " query=" + qry.dim);
            }

            final int kFinal = Math.min(Math.max(1, K), base.count);   // <-- make it final
            final int Q = qry.count;                                   // good practice to mark these final too
            final int[][] rows = new int[Q][];

            final AtomicInteger nextQ = new AtomicInteger(0);
            ExecutorService exec = Executors.newFixedThreadPool(Math.max(1, threads));
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                fs.add(exec.submit(() -> {
                    float[] qbuf = new float[base.dim];
                    while (true) {
                        int qi = nextQ.getAndIncrement();
                        if (qi >= Q) break;
                        qry.read(qi, qbuf);
                        HeapK heap = new HeapK(kFinal);                 // <-- use kFinal inside lambda
                        for (int bi = 0; bi < base.count; bi++) {
                            heap.offer(bi, base.l2(qbuf, bi));
                        }
                        rows[qi] = heap.idsAscending();
                        if ((qi + 1) % 1000 == 0) {
                            System.out.printf("  [%d/%d] queries processed%n", qi + 1, Q);
                        }
                    }
                }));
            }
            for (Future<?> f : fs) f.get();
            exec.shutdown();

            writeIvecs(outPath, rows);
        }
        long t1 = System.currentTimeMillis();
        System.out.printf(Locale.ROOT, "GT done in %.2f s%n", (t1 - t0) / 1000.0);
        return outPath;
    }

    /** If outPath is null, write next to the query file (query.ivecs). */
    public static Path run(Path basePath, Path queryPath, Integer K, Integer threads) throws Exception {
        Path out = defaultOut(queryPath);
        return run(basePath, queryPath, out,
                K != null ? K : 100,
                threads != null ? threads : Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
    }

    public static Path defaultOutputForQuery(Path queryPath) { return defaultOut(queryPath); }
}
