package com.fspann.loader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamingBatchLoader {
    private final Iterator<double[]> it;
    private final int batchSize;

    public StreamingBatchLoader(Iterator<double[]> it, int batchSize) {
        this.it = it;
        this.batchSize = batchSize;
    }

    public List<double[]> nextBatch() {
        List<double[]> batch = new ArrayList<>(batchSize);
        while (it.hasNext() && batch.size() < batchSize) {
            batch.add(it.next());
        }
        return batch;
    }
}



