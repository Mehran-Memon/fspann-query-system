package com.fspann.common;

/**
 * Represents a single result in ANN search: an identifier and its distance.
 */
public class QueryResult implements Comparable<QueryResult> {
    private final String id;
    private final double distance;

    public QueryResult(String id, double distance) {
        this.id = id;
        this.distance = distance;
    }

    public String getId() { return id; }
    public double getDistance() { return distance; }

    @Override
    public int compareTo(QueryResult other) {
        return Double.compare(this.distance, other.distance);
    }

    @Override
    public String toString() {
        return "QueryResult{id='" + id + "', distance=" + distance + '}';
    }
}
