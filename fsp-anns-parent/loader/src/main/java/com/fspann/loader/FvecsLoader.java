package com.fspann.loader;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class FvecsLoader implements FormatLoader {
    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(path)) {
            byte[] dimBuf = new byte[4];
            while (fis.read(dimBuf) == 4) {
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                byte[] vecBuf = new byte[dim * 4];
                int read = fis.read(vecBuf);
                if (read != dim * 4) break;
                float[] tmp = new float[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(tmp);
                double[] vec = new double[dim];
                for (int i = 0; i < dim; i++) vec[i] = tmp[i];
                data.add(vec);
                if (batchSize > 0 && data.size() >= batchSize) {
                    List<double[]> batch = new ArrayList<>(data);
                    data.clear();
                    return batch;
                }
            }
        }
        return data; // Return remaining data if < batchSize
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) {
        throw new UnsupportedOperationException("FVECS does not support loading indices");
    }
}