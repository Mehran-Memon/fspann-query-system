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

/**
 * Loader for FVECS format: binary float vectors with each entry prefixed by dimension.
 * Now supports streaming with per-path offset.
 */
public class FvecsLoader implements FormatLoader {
    private static final Logger logger = LoggerFactory.getLogger(FvecsLoader.class);
    private final Map<String, Integer> fileOffsets = new ConcurrentHashMap<>();

    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>(batchSize);
        int offset = fileOffsets.getOrDefault(path, 0);

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path))) {
            // Skip past already-read vectors
            for (int i = 0; i < offset; i++) {
                int dim = readInt(bis);
                if (dim <= 0) throw new IOException("Invalid vector dimension while skipping: " + dim);
                long skipped = bis.skip(dim * 4L);
                if (skipped < dim * 4L) break;
            }

            for (int i = 0; i < batchSize; i++) {
                byte[] dimBuf = new byte[4];
                if (bis.read(dimBuf) != 4) break;
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0) throw new IOException("Invalid vector dimension: " + dim);

                byte[] vecBuf = new byte[dim * 4];
                int read = bis.read(vecBuf);
                if (read != dim * 4) break;

                float[] tmp = new float[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(tmp);

                double[] vec = new double[dim];
                for (int j = 0; j < dim; j++) vec[j] = tmp[j];

                data.add(vec);
            }
        }

        fileOffsets.put(path, offset + data.size());
        return data;
    }

    private int readInt(BufferedInputStream bis) throws IOException {
        byte[] buf = new byte[4];
        if (bis.read(buf) != 4) throw new IOException("Unexpected end of file while reading int");
        return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) {
        throw new UnsupportedOperationException("FVECS does not support loading indices");
    }
}
