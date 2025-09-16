package com.fspann.index.paper;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * TagQuery (Algorithm-3)
 * ----------------------
 * Locate the subset(s) for a query code C_q using the map index I.
 * - If C_q ∈ [C_low, C_up] of some subset -> return { t }.
 * - If C_q falls in the critical area (between two adjacent subsets) -> return { t_left, t_right }.
 *
 * I is assumed sorted by lower bound.
 */
public final class TagQuery {
    private TagQuery() {}

    public static List<String> buildTags(BitSet Cq, List<GreedyPartitioner.SubsetBounds> I) {
        if (I == null || I.isEmpty()) return Collections.emptyList();
        int codeBits = I.get(0).codeBits;
        GreedyPartitioner.CodeComparator cmp = new GreedyPartitioner.CodeComparator(codeBits);

        // Binary search by lower bound
        int lo = 0, hi = I.size() - 1, pos = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            GreedyPartitioner.SubsetBounds sb = I.get(mid);
            int c = cmp.compare(Cq, sb.lower);
            if (c < 0) {
                hi = mid - 1;
            } else {
                pos = mid; // Cq >= lower(mid)
                lo = mid + 1;
            }
        }

        if (pos < 0) {
            // Cq is lexicographically smaller than the first lower bound.
            // Critical area with the first subset: return its tag and (optionally) nothing on the left.
            List<String> out = new ArrayList<>(1);
            out.add(I.get(0).tag);
            return out;
        }

        GreedyPartitioner.SubsetBounds sb = I.get(pos);
        int withinUpper = cmp.compare(Cq, sb.upper);
        if (withinUpper <= 0) {
            // Inside [lower, upper]
            return Collections.singletonList(sb.tag);
        } else {
            // Between subsets pos and pos+1 (critical area). Return both tags if next exists.
            if (pos + 1 < I.size()) {
                List<String> out = new ArrayList<>(2);
                out.add(sb.tag);
                out.add(I.get(pos + 1).tag);
                return out;
            } else {
                // After the last subset -> return the last tag
                return Collections.singletonList(sb.tag);
            }
        }
    }
}
