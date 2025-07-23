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
public class IvecsLoader implements FormatLoader {
    @Override public Iterator<double[]> openVectorIterator(Path file) {
        throw new UnsupportedOperationException("IVECS has no vectors");
    }

    @Override
    public Iterator<int[]> openIndexIterator(Path file) throws IOException {
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file))
        );
        return new Iterator<>() {
            private int[] next = readOne();
            private int[] readOne() {
                try {
                    int dim = Integer.reverseBytes(in.readInt());
                    int[] v = new int[dim];
                    for(int i=0;i<dim;i++){
                        v[i] = Integer.reverseBytes(in.readInt());
                    }
                    return v;
                } catch(EOFException eof) {
                    close(); return null;
                } catch(IOException e) {
                    close();
                    throw new UncheckedIOException(e);
                }
            }
            private void close() {
                try{ in.close(); } catch(IOException ignored){}
            }
            @Override public boolean hasNext() { return next!=null; }
            @Override public int[] next() {
                if(next==null) throw new NoSuchElementException();
                int[] v=next; next=readOne(); return v;
            }
        };
    }
}
