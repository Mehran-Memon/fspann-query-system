package com.fspann.loader;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class IvecsLoader implements FormatLoader {
    @Override
    public List<double[]> loadVectors(String path, int batchSize) {
        throw new UnsupportedOperationException("IVECS does not support loading vectors");
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) throws IOException {
        List<int[]> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(path)) {
            byte[] dimBuf = new byte[4];
            while (fis.read(dimBuf) == 4) {
                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                byte[] vecBuf = new byte[dim * 4];
                int read = fis.read(vecBuf);
                if (read != dim * 4) break;
                int[] vec = new int[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer().get(vec);
                data.add(vec);
            }
        }
        return data;
    }
}
