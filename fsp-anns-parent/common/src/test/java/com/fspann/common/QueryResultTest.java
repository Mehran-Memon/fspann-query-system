// File: src/test/java/com/fspann/common/QueryResultTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class QueryResultTest {
    @Test
    void orderingByDistance() {
        List<QueryResult> results = new ArrayList<>(List.of(
                new QueryResult("a", 2.0),
                new QueryResult("b", 1.0),
                new QueryResult("c", 3.0)
        ));
        Collections.sort(results);
        assertEquals(List.of("b","a","c"), results.stream().map(QueryResult::getId).toList());
    }

}
