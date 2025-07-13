package com.fspann.loader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class NpzLoader implements FormatLoader {
    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        throw new UnsupportedOperationException("NPZ loader not implemented");
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) throws IOException {
        throw new UnsupportedOperationException("NPZ loader not implemented");
    }
}