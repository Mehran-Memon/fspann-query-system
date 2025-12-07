package com.fspann.query.core;

import java.util.Map;

public final class Aggregates {

    // ---------- accuracy ----------
    public double avgRatio;
    public double avgServerMs;
    public double avgClientMs;
    public double avgDecryptMs;
    public double avgTokenBytes;
    public double avgWorkUnits;

    // ---------- candidate metrics ----------
    public double avgCandTotal;
    public double avgCandKept;
    public double avgCandDecrypted;
    public double avgReturned;

    // ---------- precision@K ----------
    public Map<Integer, Double> precisionAtK;

    // ---------- re-encryption cost ----------
    public long reencryptCount;
    public long reencryptBytes;
    public double reencryptMs;

    // ---------- space ----------
    public long spaceMetaBytes;
    public long spacePointsBytes;

    // ---------- ART (server + client) ----------
    public double getAvgArtMs() {
        return avgServerMs + avgClientMs;
    }

    public Aggregates() {}
}
