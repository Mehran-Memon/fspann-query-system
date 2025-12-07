package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;
import java.util.BitSet;
import java.util.Objects;

public final class CodedPoint {
    public final EncryptedPoint pt;
    public final BitSet[] codes; // length == ℓ

    public CodedPoint(EncryptedPoint pt, BitSet[] codes, int expectedDivisions)
    {
        this.pt = Objects.requireNonNull(pt, "pt");
        this.codes = Objects.requireNonNull(codes, "codes").clone();
        if (expectedDivisions <= 0) throw new IllegalArgumentException("divisions must be > 0");
        if (this.codes.length != expectedDivisions) {
            throw new IllegalArgumentException("codes length must equal divisions (ℓ)");
        }
        for (int i = 0; i < this.codes.length; i++) {
            if (this.codes[i] == null) throw new IllegalArgumentException("codes[" + i + "] is null");
        }
    }
}
