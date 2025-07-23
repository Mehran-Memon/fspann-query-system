package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loader for FVECS format: binary float vectors with each entry prefixed by dimension.
 * Now supports streaming with per-path offset.
 */
public class FvecsLoader implements FormatLoader {
    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file))
        );
        return new Iterator<>() {
            private double[] next = readOne();
            private double[] readOne() {
                try {
                    int dim = Integer.reverseBytes(in.readInt());
                    double[] v = new double[dim];
                    for(int i=0;i<dim;i++){
                        v[i] = Float.intBitsToFloat(Integer.reverseBytes(in.readInt()));
                    }
                    return v;
                } catch (EOFException eof) {
                    close(); return null;
                } catch (IOException e) {
                    close();
                    throw new UncheckedIOException(e);
                }
            }
            private void close() {
                try { in.close(); } catch(IOException ignored){}
            }
            @Override public boolean hasNext() { return next != null; }
            @Override public double[] next() {
                if(next==null) throw new NoSuchElementException();
                double[] v=next; next=readOne(); return v;
            }
        };
    }

    @Override public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("FVECS has no indices");
    }
}
