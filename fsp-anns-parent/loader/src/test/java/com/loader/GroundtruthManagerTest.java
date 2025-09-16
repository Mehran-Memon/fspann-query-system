package com.loader;

import com.fspann.loader.GroundtruthManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

class GroundtruthManagerTest {

    @SuppressWarnings("unchecked")
    @Test
    void normalizeIndexBaseIfNeeded_convertsOneBasedToZeroBased() throws Exception {
        GroundtruthManager gt = new GroundtruthManager();

        // Inject rows via reflection to avoid file I/O
        Field rowsF = GroundtruthManager.class.getDeclaredField("rows");
        rowsF.setAccessible(true);
        rowsF.set(gt, java.util.List.of(
                new int[]{1, 2, 3},
                new int[]{3, 3, 1},     // includes a duplicate on purpose
                new int[]{2}
        ));

        // dataset size is 3, GT is clearly 1-based [1..3]
        gt.normalizeIndexBaseIfNeeded(3);

        int[] r0 = gt.getGroundtruth(0);
        int[] r1 = gt.getGroundtruth(1);
        int[] r2 = gt.getGroundtruth(2);

        // Expect each entry decremented by 1
        assertArrayEquals(new int[]{0,1,2}, r0);
        assertArrayEquals(new int[]{2,2,0}, r1);
        assertArrayEquals(new int[]{1},     r2);

        // After normalization, all ids should be within [0, 3)
        assertTrue(gt.isConsistentWithDatasetSize(3));
    }

    @Test
    void isConsistentWithDatasetSize_flagsOutOfRangeIds() throws Exception {
        GroundtruthManager gt = new GroundtruthManager();

        // rows = [[0, 1], [99]] with datasetSize=10 should be inconsistent
        Field rowsF = GroundtruthManager.class.getDeclaredField("rows");
        rowsF.setAccessible(true);
        rowsF.set(gt, java.util.List.of(
                new int[]{0, 1},
                new int[]{99}
        ));

        assertFalse(gt.isConsistentWithDatasetSize(10));
    }
}
