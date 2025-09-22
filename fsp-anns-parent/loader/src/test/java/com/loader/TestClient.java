package com.loader;

import com.fspann.loader.DefaultDataLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestClient {
    public static void main(String[] args) throws IOException {
        DefaultDataLoader loader = new DefaultDataLoader();
        Path dataFile = Paths.get("E:/Research Work/Datasets/sift_dataset/sift_base.fvecs");
        int batchSize = 100;

        List<double[]> batch;
        int total = 0;
        do {
            batch = loader.loadData(dataFile.toString(), batchSize);
            for (double[] vec : batch) {
                System.out.println(java.util.Arrays.toString(vec));
            }
            total += batch.size();
        } while (batch.size() == batchSize);

        System.out.println("Total vectors read: " + total);
    }
}
