package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.index.paper.PartitionedIndexService;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Updated tests for PartitionedIndexService under the new Option-C design.
 *
 * Notes:
 * - PartitionedIndexService is no longer part of actual querying.
 * - It is now an ablation-only component.
 * - lookup() returns empty unless codes + full pipeline exist (not used in Option-C).
 * - No plaintext → codes embedding happens in production.
 */
class PartitionedIndexServiceTest {

    private static EncryptedPoint ep(String id, int dim) {
        return new EncryptedPoint(
                id,
                0,
                new byte[12],
                new byte[16],
                1,
                dim,
                Collections.emptyList()
        );
    }

    @Test
    void lookup_withoutCodes_returnsEmpty() {
        int dim = 4;

        PartitionedIndexService svc = new PartitionedIndexService(
                8, 4, 2, 42L
        );

        double[] vec = {0.9, 0.1, -0.2, 0.7};
        svc.insert(ep("secret-id", dim), vec);

        QueryToken token = new QueryToken(
                Collections.emptyList(),
                null,                 // NO CODES
                new byte[12],
                new byte[16],
                10,
                0,
                "epoch_1_dim_4",
                dim,
                1
        );

        var out = svc.lookup(token);
        assertTrue(out.isEmpty());
    }

    @Test
    void insertAndCountOnly_behaviour() {
        int dim = 4;

        PartitionedIndexService svc = new PartitionedIndexService(
                8, 4, 3,
                1234L,
                1,
                -1
        );

        EncryptedPoint p1 = ep("a", dim);
        EncryptedPoint p2 = ep("b", dim);

        svc.insert(p1, new double[]{1,2,3,4});
        svc.insert(p2, new double[]{5,6,7,8});

        assertEquals(2, svc.getVectorCountForDimension(dim));

        svc.delete("a");
        assertEquals(1, svc.getVectorCountForDimension(dim));

        svc.delete("b");
        assertEquals(0, svc.getVectorCountForDimension(dim));
    }
}
