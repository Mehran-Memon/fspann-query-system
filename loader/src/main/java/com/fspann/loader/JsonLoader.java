package com.fspann.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class JsonLoader implements FormatLoader {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode node = mapper.readTree(line);
                double[] vec = new double[node.size()];
                for (int i = 0; i < node.size(); i++) {
                    vec[i] = node.get(i).asDouble();
                }
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
        throw new UnsupportedOperationException("JSON does not support loading indices");
    }
}