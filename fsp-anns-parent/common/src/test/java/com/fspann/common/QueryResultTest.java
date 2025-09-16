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

        List<String> sortedIds = results.stream().map(QueryResult::getId).toList();
        List<Double> sortedDistances = results.stream().map(QueryResult::getDistance).toList();

        assertEquals(List.of("b", "a", "c"), sortedIds);
        assertEquals(List.of(1.0, 2.0, 3.0), sortedDistances);
    }

    @Test
    void equalityAndComparison() {
        QueryResult r1 = new QueryResult("x", 1.23);
        QueryResult r2 = new QueryResult("y", 1.23);
        QueryResult r3 = new QueryResult("z", 4.56);

        assertEquals(0, r1.compareTo(r2)); // equal distances
        assertTrue(r1.compareTo(r3) < 0);  // r1 closer than r3
        assertTrue(r3.compareTo(r2) > 0);  // r3 farther than r2
    }

    @Test
    void gettersReturnCorrectValues() {
        QueryResult result = new QueryResult("q42", 9.99);
        assertEquals("q42", result.getId());
        assertEquals(9.99, result.getDistance());
    }
}
