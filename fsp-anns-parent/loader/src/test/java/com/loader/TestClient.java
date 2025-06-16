package com.loader;

import com.fspann.loader.DefaultDataLoader;
import java.util.List;

public class TestClient {
    public static void main(String[] args) throws Exception {
        DefaultDataLoader loader = new DefaultDataLoader();
        String path = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs"; // Replace with actual path or use a test file
        int batchSize = 100;
        List<double[]> vectors;
        do {
            vectors = loader.loadData(path, batchSize);
            for (double[] vec : vectors) {
                // Process each vector (e.g., index it)
                System.out.println(java.util.Arrays.toString(vec));
            }
        } while (vectors.size() == batchSize); // Continue until < batchSize
    }
}
