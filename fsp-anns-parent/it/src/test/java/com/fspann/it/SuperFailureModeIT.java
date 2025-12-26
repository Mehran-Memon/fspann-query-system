package com.fspann.it;

import com.fspann.index.paper.GFunctionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SuperFailureModeIT extends BaseUnifiedIT {

    @Test
    @DisplayName("Query before finalize fails")
    void queryBeforeFinalizeFails() {

        system.insert("x", new double[DIM], DIM);

        double[] q = new double[DIM];
        assertThrows(
                IllegalStateException.class,
                () -> system.createToken(q, 5, DIM)
        );
    }

    @Test
    @DisplayName("Dimension mismatch fails fast")
    void dimensionMismatchFails() {

        assertThrows(
                IllegalArgumentException.class,
                () -> system.insert("bad", new double[DIM+1], DIM)
        );
    }

    @Test
    @DisplayName("GFunctionRegistry mismatch fails")
    void registryMismatchFails() {

        GFunctionRegistry.reset(); // sabotage

        double[] q = new double[DIM];

        assertThrows(
                IllegalStateException.class,
                () -> system.createToken(q, 5, DIM)
        );
    }
}
