package com.fspann.common;

import java.util.Map;

public record SearchDiagnostics(
        int uniqueCandidates,
        int probedBuckets,
        Map<Integer,Integer> fanoutPerTable // tableId -> #unique IDs contributed
) {
    public static final SearchDiagnostics EMPTY =
            new SearchDiagnostics(0, 0, Map.of());
}
