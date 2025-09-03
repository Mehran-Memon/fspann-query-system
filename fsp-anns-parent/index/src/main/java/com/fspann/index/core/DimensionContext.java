package com.fspann.index.core;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.Coding;
import com.fspann.index.paper.GreedyPartitioner;
import com.fspann.index.paper.TagQuery;

import java.io.Serializable;
import java.util.List;

/**
 * DimensionContext
 *
 * Holds per-dimension services and parameters needed for
 * secure, forward-secure ANN querying in the SANNP / mSANNP framework.
 *
 * In the legacy system this wrapped an EvenLSH router and SecureLSHIndex.
 * In the paper-aligned design, we maintain:
 *   - CryptoService for AES-GCM encryption/decryption,
 *   - KeyLifeCycleService for forward-secure key rotation,
 *   - Coding.GFunction (LSH family) per Algorithm-1,
 *   - GreedyPartitioner + TagQuery to enable partitioned index lookup.
 *
 * See:
 *   Peng et al., "Towards Secure Approximate k-NN Query Over Encrypted
 *   High-Dimensional Data" (IEEE Access 2018), Sections III–IV. :contentReference[oaicite:2]{index=2}
 *   Dissertation Proposal, "Forward-Secure and Privacy-Preserving ANN Query
 *   Framework (FSP-ANN)" (Xidian Univ., 2024). :contentReference[oaicite:3]{index=3}
 */
public class DimensionContext implements Serializable {
    private final int dimension;
    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    // Parameters and artifacts for coding/partitioning
    private final Coding.GFunction gFunction;
    private final List<GreedyPartitioner.SubsetBounds> mapIndex;
    private final int partitionWidth;

    public DimensionContext(int dimension,
                            CryptoService crypto,
                            KeyLifeCycleService keyService,
                            Coding.GFunction gFunction,
                            List<GreedyPartitioner.SubsetBounds> mapIndex,
                            int partitionWidth) {
        this.dimension = dimension;
        this.crypto = crypto;
        this.keyService = keyService;
        this.gFunction = gFunction;
        this.mapIndex = mapIndex;
        this.partitionWidth = partitionWidth;
    }

    /** Dimension of vectors handled in this context. */
    public int getDimension() { return dimension; }

    /** AES-GCM crypto service (IND-CPA secure, authenticated). */
    public CryptoService getCrypto() { return crypto; }

    /** Forward-secure key lifecycle service. */
    public KeyLifeCycleService getKeyService() { return keyService; }

    /** LSH coding function family (Algorithm-1). */
    public Coding.GFunction getGFunction() { return gFunction; }

    /** Greedy partition map index (Algorithm-2). */
    public List<GreedyPartitioner.SubsetBounds> getMapIndex() { return mapIndex; }

    /** Partition width w (max size of equal-code set). */
    public int getPartitionWidth() { return partitionWidth; }

    /**
     * Legacy method kept for binary compatibility. In this architecture,
     * re-encryption is handled globally by KeyRotationService + MetadataManager.
     */
    @Deprecated
    public void reEncryptAll() {
        // No-op. Key rotation and re-encryption are coordinated at system level.
    }

    /**
     * Generate cloak query tags for a given plaintext query vector.
     * Client-side only. See Algorithm-3 (TagQuery).
     *
     * @param q plaintext query vector
     * @return list of one or two tags to fetch from server
     */
    public List<String> buildQueryTags(double[] q) {
        return TagQuery.buildTags(Coding.C(q, gFunction), mapIndex);
    }
}
