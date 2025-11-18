package com.fspann.common;

import java.util.List;

public record LookupWithDiagnostics(
        List<EncryptedPoint> candidates,   // what lookup() returns today
        SearchDiagnostics diagnostics      // union size / fanout you’ll fill later
) {
    public static LookupWithDiagnostics of(List<EncryptedPoint> c) {
        return new LookupWithDiagnostics(
                c != null ? c : List.of(),
                SearchDiagnostics.EMPTY
        );
    }
}
