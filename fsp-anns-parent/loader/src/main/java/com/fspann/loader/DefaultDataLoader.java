package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of DataLoader that delegates to format-specific loaders.
 */
public class DefaultDataLoader implements DataLoader {
    private static final Logger logger = LoggerFactory.getLogger(DefaultDataLoader.class);
    private final Map<String, FormatLoader> registry = new HashMap<>();

    public DefaultDataLoader() {
        // Register built-in format loaders
        registry.put("CSV",  new CsvLoader());
        registry.put("JSON", new JsonLoader());
        registry.put("FVECS", new FvecsLoader());
        registry.put("IVECS", new IvecsLoader());
        registry.put("NPZ",  new NpzLoader());
        registry.put("PARQUET", new ParquetLoader());
    }

    @Override
    public List<double[]> loadData(String path, int batchSize) throws IOException {
        String ext = detectExtension(path);
        FormatLoader loader = registry.get(ext);
        if (loader == null) {
            throw new UnsupportedOperationException("Unsupported format: " + ext);
        }
        return loader.loadVectors(path, batchSize);
    }

    @Override
    public List<int[]> loadGroundTruth(String path, int batchSize) throws IOException {
        FormatLoader loader = registry.get("IVECS");
        return loader.loadIndices(path, batchSize);
    }

    private String detectExtension(String path) {
        int idx = path.lastIndexOf('.') + 1;
        if (idx <= 0 || idx >= path.length()) return "";
        return path.substring(idx).toUpperCase();
    }
}