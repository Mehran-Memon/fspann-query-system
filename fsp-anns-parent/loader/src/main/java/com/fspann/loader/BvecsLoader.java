// loader/src/main/java/com/fspann/loader/BvecsLoader.java
package com.fspann.loader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Loader for BVECs format: binary unsigned-byte vectors with each entry
 * prefixed by the dimension (little-endian int).
 *
 * Record layout per vector:
 *   int32 dim (LE), then dim bytes (uint8, 0..255)
 *
 * Returned as double[] (0..255) to match the rest of the pipeline.
 * Use with StreamingBatchLoader for large datasets (e.g., SIFT1B).
 */
public class BvecsLoader implements FormatLoader {

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file))
        );
        return new Iterator<>() {
            private double[] next = readOne();

            private double[] readOne() {
                try {
                    int dim = Integer.reverseBytes(in.readInt()); // BVECs uses LE
                    if (dim <= 0 || dim > 1_000_000) {
                        throw new IOException("Invalid dimension: " + dim);
                    }
                    byte[] buf = new byte[dim];
                    in.readFully(buf);

                    double[] v = new double[dim];
                    for (int i = 0; i < dim; i++) {
                        v[i] = buf[i] & 0xFF; // unsigned → 0..255
                    }
                    return v;
                } catch (EOFException eof) {
                    closeQuietly();
                    return null;
                } catch (IOException e) {
                    closeQuietly();
                    throw new UncheckedIOException(e);
                }
            }

            private void closeQuietly() {
                try { in.close(); } catch (IOException ignored) {}
            }

            @Override public boolean hasNext() { return next != null; }

            @Override
            public double[] next() {
                if (next == null) throw new NoSuchElementException();
                double[] v = next;
                next = readOne();
                return v;
            }
        };
    }

    @Override
    public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("BVECs has no indices");
    }
}
