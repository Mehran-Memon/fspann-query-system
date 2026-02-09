package com.fspann.loader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

public class FvecsLoader implements FormatLoader {

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        FileChannel channel = FileChannel.open(file);
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.flip();

        return new Iterator<>() {
            private double[] next = readOne();

            private double[] readOne() {
                try {
                    if (buffer.remaining() < 4) {
                        if (!refill()) return null;
                    }

                    int dim = buffer.getInt();

                    // VALIDATION FOR UNIT TESTS
                    if (dim <= 0 || dim > 1_000_000) {
                        throw new IOException("Invalid dimension: " + dim);
                    }

                    int bytesNeeded = dim * 4;
                    if (buffer.remaining() < bytesNeeded) {
                        if (!refill()) return null;
                    }

                    double[] v = new double[dim];
                    for (int i = 0; i < dim; i++) {
                        v[i] = buffer.getFloat();
                    }
                    return v;
                } catch (IOException e) {
                    close();
                    // Wrap in UncheckedIOException to satisfy FvecsLoaderTest
                    throw new UncheckedIOException(e);
                } catch (Exception e) {
                    close();
                    return null;
                }
            }

            private boolean refill() throws IOException {
                buffer.compact();
                int read = channel.read(buffer);
                buffer.flip();
                return read != -1;
            }

            private void close() {
                try { channel.close(); } catch (IOException ignored) {}
            }

            @Override public boolean hasNext() { return next != null; }
            @Override public double[] next() {
                if (next == null) throw new NoSuchElementException();
                double[] v = next;
                next = readOne();
                if (next == null) close();
                return v;
            }
        };
    }

    @Override public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("FVECS has no indices");
    }
}