package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IvecsLoader implements FormatLoader {
    private static final Logger logger = LoggerFactory.getLogger(IvecsLoader.class);
    private final Map<String, Integer> fileOffsets = new ConcurrentHashMap<>();

    @Override
    public List<double[]> loadVectors(String path, int batchSize) {
        throw new UnsupportedOperationException("IVECS does not support loading vectors");
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) throws IOException {
        List<int[]> batch = new ArrayList<>(batchSize);
        int offset = fileOffsets.getOrDefault(path, 0);

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path))) {
            // Skip previously read vectors
            for (int i = 0; i < offset; i++) {
                int dim = readInt(bis);
                if (dim <= 0) throw new IOException("Invalid dimension while skipping: " + dim);
                long skipped = bis.skip(dim * 4L);
                if (skipped < dim * 4L) break;
            }

            for (int i = 0; i < batchSize; i++) {
                byte[] dimBuf = new byte[4];
                if (bis.read(dimBuf) != 4) break;
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0) throw new IOException("Invalid dimension: " + dim);

                byte[] vecBuf = new byte[dim * 4];
                int read = bis.read(vecBuf);
                if (read != dim * 4) break;

                int[] vec = new int[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(vec);
                batch.add(vec);
            }
        }

        fileOffsets.put(path, offset + batch.size());
        return batch;
    }

    private int readInt(BufferedInputStream bis) throws IOException {
        byte[] buf = new byte[4];
        if (bis.read(buf) != 4) throw new IOException("Unexpected EOF while reading dimension");
        return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
