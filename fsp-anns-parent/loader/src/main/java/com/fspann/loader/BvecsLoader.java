package com.fspann.loader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

public class BvecsLoader implements FormatLoader {
    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        FileChannel channel = FileChannel.open(file);
        // We use a Direct Buffer to bypass JVM heap for raw IO
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB chunk
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.flip(); // Set to "empty" to trigger first read

        return new Iterator<>() {
            private double[] next = readOne();

            private double[] readOne() {
                try {
                    // Check if we have enough for the 4-byte dimension header
                    if (buffer.remaining() < 4) {
                        if (!refill()) return null;
                    }

                    int dim = buffer.getInt();
                    if (dim <= 0 || dim > 10000) return null; // Sanity check

                    // Ensure the vector data is in the buffer
                    if (buffer.remaining() < dim) {
                        if (!refill()) return null;
                    }

                    double[] v = new double[dim];
                    for (int i = 0; i < dim; i++) {
                        v[i] = buffer.get() & 0xFF; // Unsigned uint8
                    }
                    return v;
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
                double[] v = next;
                next = readOne();
                if (next == null) close();
                return v;
            }
        };
    }

    @Override public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException();
    }
}