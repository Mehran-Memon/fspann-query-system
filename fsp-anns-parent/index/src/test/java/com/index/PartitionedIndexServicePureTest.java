package com.index;

import com.fspann.common.*;
import com.fspann.index.paper.PartitionedIndexService;
import org.junit.jupiter.api.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class PartitionedIndexServicePureTest {

    @Test
    void insert_storesPointsPerDimension() {
        PartitionedIndexService eng = new PartitionedIndexService(3, 2, 2, 13L);

        EncryptedPoint p1 = new EncryptedPoint("a",0,new byte[12],new byte[16],1,2,List.of());
        EncryptedPoint p2 = new EncryptedPoint("b",0,new byte[12],new byte[16],1,2,List.of());

        eng.insert(p1, new double[]{1,2});
        eng.insert(p2, new double[]{3,4});

        assertEquals(2, eng.getVectorCountForDimension(2));
        assertEquals(2, eng.getTotalVectorCount());
    }

    @Test
    void lookup_returnsEmpty_ifNoCodesMatch() {
        PartitionedIndexService eng = new PartitionedIndexService(3, 2, 2, 13L);

        QueryToken tok = new QueryToken(List.of(), null, new byte[12], new byte[16], 5,1,"ctx",2,1);
        assertTrue(eng.lookup(tok).isEmpty());
    }
}
