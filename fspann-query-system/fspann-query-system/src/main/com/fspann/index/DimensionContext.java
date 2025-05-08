package com.fspann.index;

import com.fspann.keymanagement.KeyManager;
import java.util.List;

public class DimensionContext {
    private EvenLSH lsh;
    private ANN ann;
    private SecureLSHIndex index;

    private int dimensions;  // Store number of dimensions for each context
    private int numIntervals; // Number of intervals for LSH (added this to support update)

    public DimensionContext(int dimensions, int numIntervals, KeyManager keyManager, List<double[]> baseVectors) {
        this.dimensions = dimensions;
        this.numIntervals = numIntervals;
        this.lsh = new EvenLSH(dimensions, numIntervals);  // Initialize EvenLSH with specific dimensions
        this.ann = new ANN(dimensions, 100, keyManager, baseVectors); // Assuming 100 buckets here
        this.index = new SecureLSHIndex(3, keyManager.getCurrentKey(), baseVectors);  // Secure index that ties to the KeyManager for encryption
    }

    /**
     * Dynamically updates the context if dimensions or configuration changes.
     * Useful when handling multiple datasets with different dimensionalities.
     */
    public void updateContext(int newDimensions, KeyManager keyManager, List<double[]> baseVectors) {
        this.dimensions = newDimensions;
        this.lsh = new EvenLSH(newDimensions, this.numIntervals);  // Use the same number of intervals
        this.ann = new ANN(newDimensions, 100, keyManager, baseVectors); // Reinitialize ANN with the new dimensions
        this.index = new SecureLSHIndex(3, keyManager.getCurrentKey(), baseVectors);  // Reinitialize index with new key
    }

    // Getter methods to retrieve the current LSH, ANN, and index
    public EvenLSH getLsh() {
        return lsh;
    }

    public ANN getAnn() {
        return ann;
    }

    public SecureLSHIndex getIndex() {
        return index;
    }
}
