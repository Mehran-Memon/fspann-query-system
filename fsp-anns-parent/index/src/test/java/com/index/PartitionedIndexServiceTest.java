package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.index.paper.PartitionedIndexService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the paper / partitioned index engine:
 *  - basic insert + lookup round-trip
 *  - vector count per dimension
 *  - behavior when no codes are supplied in the QueryToken
 */
class PartitionedIndexServiceTest {

    /**
     * Helper to create a minimal EncryptedPoint for a given dimension.
     * Per-table buckets are irrelevant in paper mode.
     */
    private static EncryptedPoint ep(String id, int dim) {
        return new EncryptedPoint(
                id,
                /*shardId*/ 0,
                /*iv*/ new byte[12],
                /*cipher*/ new byte[16],
                /*version*/ 1,
                /*vectorLength*/ dim,
                /*perTableBuckets*/ Collections.emptyList()
        );
    }

    @Test
    void insertWithPlaintext_thenLookup_returnsInsertedPoint() {
        int dim = 4;

        // Small, deterministic config for easy testing
        PartitionedIndexService svc = new PartitionedIndexService(
                /*m*/ 8,
                /*lambda*/ 4,
                /*divisions*/ 3,
                /*seedBase*/ 1234L,
                /*buildThreshold*/ 1,   // build as soon as first point arrives
                /*maxCandidates*/ -1
        );

        double[] vec = new double[]{1.0, -0.5, 0.3, 0.7};
        EncryptedPoint pt = ep("p1", dim);

        // Deprecated but still-supported path: server computes codes from plaintext
        svc.insert(pt, vec);   // internally builds partitions because buildThreshold=1

        // Build codes for a query close to the inserted vector
        double[] q = new double[]{1.0, -0.5, 0.3, 0.7};
        BitSet[] codes = svc.code(q);

        // Paper/partitioned mode ignores per-table buckets; pass an empty list
        List<List<Integer>> perTableBuckets = Collections.emptyList();

        QueryToken token = new QueryToken(
                perTableBuckets,
                codes,
                /*iv*/ new byte[12],
                /*cipher*/ new byte[16],
                /*topK*/ 10,
                /*numTables*/ 0,
                /*encCtx*/ "epoch_1_dim_" + dim,
                /*dimension*/ dim,
                /*version*/ 1
        );

        var out = svc.lookup(token);
        assertFalse(out.isEmpty(), "Lookup should return at least one candidate");
        assertTrue(out.stream().anyMatch(p -> "p1".equals(p.getId())),
                "Inserted point id 'p1' should appear in lookup results");
    }

    @Test
    void getVectorCountForDimension_tracksInsertAndDelete() {
        int dim = 4;

        PartitionedIndexService svc = new PartitionedIndexService(
                /*m*/ 8,
                /*lambda*/ 4,
                /*divisions*/ 2,
                /*seedBase*/ 9876L,
                /*buildThreshold*/ 1,
                /*maxCandidates*/ -1
        );

        double[] v1 = new double[]{0.1, 0.2, 0.3, 0.4};
        double[] v2 = new double[]{0.5, 0.6, 0.7, 0.8};

        EncryptedPoint p1 = ep("id-1", dim);
        EncryptedPoint p2 = ep("id-2", dim);

        svc.insert(p1, v1);
        svc.insert(p2, v2);

        int countAfterInsert = svc.getVectorCountForDimension(dim);
        assertEquals(2, countAfterInsert, "Should see 2 vectors after two inserts");

        // Delete one and re-check
        svc.delete("id-1");
        int countAfterDelete = svc.getVectorCountForDimension(dim);
        assertEquals(1, countAfterDelete, "Should see 1 vector after deleting one id");

        // Delete the remaining one
        svc.delete("id-2");
        int countAfterAllDeleted = svc.getVectorCountForDimension(dim);
        assertEquals(0, countAfterAllDeleted, "Should be 0 after deleting all vectors for the dimension");
    }

    @Test
    void lookup_withoutCodes_returnsEmpty_forPrivacy() {
        int dim = 4;

        PartitionedIndexService svc = new PartitionedIndexService(
                /*m*/ 8,
                /*lambda*/ 4,
                /*divisions*/ 2,
                /*seedBase*/ 42L,
                /*buildThreshold*/ 1,
                /*maxCandidates*/ -1
        );

        // Insert a point so the index is non-empty
        double[] vec = new double[]{0.9, 0.1, -0.2, 0.7};
        EncryptedPoint pt = ep("secret-id", dim);
        svc.insert(pt, vec);

        // Now build a token that does NOT carry any codes
        List<List<Integer>> perTableBuckets = new ArrayList<>(); // ignored
        BitSet[] noCodes = null; // this simulates a privacy-preserving query w/out paper codes

        QueryToken token = new QueryToken(
                perTableBuckets,
                noCodes,
                /*iv*/ new byte[12],
                /*cipher*/ new byte[16],
                /*topK*/ 10,
                /*numTables*/ 0,
                /*encCtx*/ "epoch_1_dim_" + dim,
                /*dimension*/ dim,
                /*version*/ 1
        );

        var out = svc.lookup(token);
        assertTrue(out.isEmpty(),
                "When no codes are supplied, partitioned engine should not leak structure and must return empty set");
    }
}
