package com.crypto;

import com.fspann.crypto.ReencryptionTracker;
import org.junit.jupiter.api.*;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class ForwardSecurityCombinatoricsPureTest {

    @Test
    void tracker_accumulatesUniqueTouches() {
        ReencryptionTracker t = new ReencryptionTracker();

        t.touch("a");
        t.touch("b");
        t.touch("a");

        assertEquals(2, t.uniqueCount());
    }

    @Test
    void drainAll_resetsInternalSet() {
        ReencryptionTracker t = new ReencryptionTracker();

        t.touch("x");
        t.touch("y");

        Set<String> drained = t.drainAll();
        assertEquals(Set.of("x","y"), drained);
        assertEquals(0, t.uniqueCount());
    }
}
