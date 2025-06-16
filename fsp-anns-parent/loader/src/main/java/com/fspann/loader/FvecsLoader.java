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

public class FvecsLoader implements FormatLoader {
    private static final Logger logger = LoggerFactory.getLogger(FvecsLoader.class);

    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path))) {
            byte[] dimBuf = new byte[4];
            while (bis.read(dimBuf) == 4) {
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0) throw new IOException("Invalid vector dimension: " + dim);

                byte[] vecBuf = new byte[dim * 4];
                int read = bis.read(vecBuf);
                if (read != dim * 4) break;

                float[] tmp = new float[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(tmp);

                double[] vec = new double[dim];
                for (int i = 0; i < dim; i++) vec[i] = tmp[i];

                data.add(vec);
                if (batchSize > 0 && data.size() >= batchSize) break;
            }
        }
        logger.info("Loaded {} vectors from {}", data.size(), path);
        return data;
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) {
        throw new UnsupportedOperationException("FVECS does not support loading indices");
    }
}
