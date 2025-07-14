package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class IvecsLoader implements FormatLoader {
    private static final Logger logger = LoggerFactory.getLogger(IvecsLoader.class);

    @Override
    public List<double[]> loadVectors(String path, int batchSize) {
        throw new UnsupportedOperationException("IVECS does not support loading vectors");
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) throws IOException {
        List<int[]> data = new ArrayList<>();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path))) {
            byte[] dimBuf = new byte[4];
            while (bis.read(dimBuf) == 4) {
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0) throw new IOException("Invalid dimension: " + dim);

                byte[] vecBuf = new byte[dim * 4];
                int read = bis.read(vecBuf);
                if (read != dim * 4) break;

                int[] vec = new int[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(vec);
                data.add(vec);
                if (batchSize > 0 && data.size() >= batchSize) break;
            }
        }
        logger.info("Loaded {} index vectors from {}", data.size(), path);
        return data;
    }
}
