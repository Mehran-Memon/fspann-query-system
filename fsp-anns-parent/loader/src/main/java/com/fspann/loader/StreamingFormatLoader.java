package com.fspann.loader;

import java.io.IOException;
import java.util.List;

public interface StreamingFormatLoader extends FormatLoader {
    /**
     * Load the next batch from a given file path (stateful).
     */
    List<double[]> loadNextBatch(String path, int batchSize) throws IOException;
}
