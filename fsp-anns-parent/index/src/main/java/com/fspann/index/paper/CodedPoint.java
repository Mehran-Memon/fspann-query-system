package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;
import java.util.BitSet;
import java.util.Objects;

/**
 * CodedPoint: MSANNP option.
 * Stores (EncryptedPoint, BitSet codes for each division).
 */
public final class CodedPoint {

    public final EncryptedPoint pt;
    public final BitSet[] codes;    // PUBLIC and immutable

    public CodedPoint(EncryptedPoint pt, BitSet[] codes, int divisions) {
        this.pt = Objects.requireNonNull(pt, "pt");
        this.codes = Objects.requireNonNull(codes, "codes").clone();

        if (divisions <= 0)
            throw new IllegalArgumentException("divisions must be > 0");

        if (this.codes.length != divisions)
            throw new IllegalArgumentException("codes length must equal divisions");

        for (int i = 0; i < divisions; i++) {
            if (this.codes[i] == null)
                throw new IllegalArgumentException("codes[" + i + "] is null");
        }
    }
}
