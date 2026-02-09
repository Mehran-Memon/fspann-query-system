package com.fspann.loader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

public class IvecsLoader implements FormatLoader {

    @Override
    public Iterator<int[]> openIndexIterator(Path file) throws IOException {
        FileChannel channel = FileChannel.open(file);
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.flip();

        return new Iterator<>() {
            private int[] next = readOne();

            private int[] readOne() {
                try {
                    if (buffer.remaining() < 4) {
                        if (!refill()) return null;
                    }

                    int k = buffer.getInt();

                    // VALIDATION FOR UNIT TESTS
                    if (k <= 0 || k > 1_000_000) {
                        throw new IOException("Invalid dimension: " + k);
                    }

                    int bytesNeeded = k * 4;
                    if (buffer.remaining() < bytesNeeded) {
                        if (!refill()) return null;
                    }

                    int[] ids = new int[k];
                    for (int i = 0; i < k; i++) {
                        ids[i] = buffer.getInt();
                    }
                    return ids;
                } catch (IOException e) {
                    close();
                    // Wrap in UncheckedIOException to satisfy IvecsLoaderTest
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
            @Override public int[] next() {
                if (next == null) throw new NoSuchElementException();
                int[] v = next;
                next = readOne();
                if (next == null) close();
                return v;
            }
        };
    }

    @Override public Iterator<double[]> openVectorIterator(Path file) {
        throw new UnsupportedOperationException("IVECS has no vectors");
    }
}