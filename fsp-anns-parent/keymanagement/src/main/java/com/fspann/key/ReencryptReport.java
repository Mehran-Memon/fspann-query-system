package com.fspann.key;

public class ReencryptReport {
    public final int touchedCount;
    public final int reencryptedCount;
    public final long timeMs;
    public final long bytesDelta;
    public final long bytesAfter;

    public ReencryptReport(int touchedCount, int reencryptedCount, long timeMs, long bytesDelta, long bytesAfter) {
        if (touchedCount < 0 || reencryptedCount < 0) throw new IllegalArgumentException("counts must be >= 0");
        if (reencryptedCount > touchedCount) throw new IllegalArgumentException("re-encrypted > touched");
        if (timeMs < 0 || bytesDelta < 0 || bytesAfter < 0) throw new IllegalArgumentException("metrics must be >= 0");
        this.touchedCount = touchedCount;
        this.reencryptedCount = reencryptedCount;
        this.timeMs = timeMs;
        this.bytesDelta = bytesDelta;
        this.bytesAfter = bytesAfter;
    }
}
