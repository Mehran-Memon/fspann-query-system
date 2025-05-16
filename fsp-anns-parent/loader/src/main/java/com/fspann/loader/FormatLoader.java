package com.fspann.loader;

import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for file-format–specific loading logic.
 */
public interface FormatLoader {
    List<double[]> loadVectors(String path, int batchSize) throws IOException;
    List<int[]>   loadIndices(String path, int batchSize) throws IOException;
}