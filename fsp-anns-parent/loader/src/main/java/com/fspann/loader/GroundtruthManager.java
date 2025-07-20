package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Loads .ivecs groundtruth file where each line contains k integer indices (nearest neighbor IDs).
 */
public class GroundtruthManager {
    private static final Logger logger = LoggerFactory.getLogger(GroundtruthManager.class);
    private final List<int[]> groundtruthList = new ArrayList<>();

    /**
     * Load all groundtruth entries from .ivecs file into memory.
     * Each row is expected to be of form: [dim][int_1][int_2]...[int_dim]
     */
    public void load(String ivecsPath) throws IOException {
        groundtruthList.clear();

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(ivecsPath))) {
            while (true) {
                byte[] dimBuf = new byte[4];
                if (bis.read(dimBuf) != 4) break;

                int dim = ByteBuffer.wrap(dimBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (dim <= 0 || dim > 10_000) {
                    logger.warn("Skipping invalid dimension {}", dim);
                    break;
                }

                byte[] vecBuf = new byte[dim * 4];
                if (bis.read(vecBuf) != vecBuf.length) break;

                int[] vec = new int[dim];
                ByteBuffer.wrap(vecBuf).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(vec);
                groundtruthList.add(vec);
            }
        }

        logger.info("Loaded {} groundtruth entries from {}", groundtruthList.size(), ivecsPath);
    }

    /**
     * Returns top-k true nearest neighbor IDs for a given query index.
     * @param queryIndex index in the groundtruth list (e.g. 0 for first query)
     * @param topK desired number of neighbors
     * @return array of top-K groundtruth vector IDs
     */
    public int[] getGroundtruth(int queryIndex, int topK) {
        if (queryIndex >= groundtruthList.size()) {
            throw new IndexOutOfBoundsException("Query index " + queryIndex + " is out of bounds.");
        }
        int[] full = groundtruthList.get(queryIndex);
        return Arrays.copyOfRange(full, 0, Math.min(topK, full.length));
    }

    public int totalQueries() {
        return groundtruthList.size();
    }
}
