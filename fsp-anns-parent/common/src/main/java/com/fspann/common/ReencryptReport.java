package com.fspann.common;

public record ReencryptReport(
        int touchedCount,
        int reencryptedCount,
        long timeMs,
        long bytesDelta,
        long bytesAfter
) {}