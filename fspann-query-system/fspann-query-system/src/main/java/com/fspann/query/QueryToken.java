package java.com.fspann.query;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class QueryToken {
    private final List<Integer> candidateBuckets;
    private final byte[] encryptedQuery;
    private final int topK;
    private final String encryptionContext;

    public QueryToken(List<Integer> candidateBuckets, byte[] encryptedQuery, int topK, String encryptionContext) {
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            throw new IllegalArgumentException("candidateBuckets cannot be null or empty");
        }
        if (topK < 0) {
            throw new IllegalArgumentException("topK cannot be negative: " + topK);
        }
        this.candidateBuckets = Collections.unmodifiableList(new ArrayList<>(candidateBuckets));
        this.encryptedQuery = encryptedQuery != null ? encryptedQuery.clone() : null;
        this.topK = topK;
        // Ensure that the encryptionContext is never null
        this.encryptionContext = encryptionContext != null ? encryptionContext : "epoch_0";
    }

    public QueryToken(List<Integer> candidateBuckets, byte[] encryptedQuery, int topK) {
        this(candidateBuckets, encryptedQuery, topK, null);
    }

    public List<Integer> getCandidateBuckets() {
        return candidateBuckets;
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery != null ? encryptedQuery.clone() : null;
    }

    public int getTopK() {
        return topK;
    }

    public String getEncryptionContext() {
        return encryptionContext;
    }

    @Override
    public String toString() {
        return "QueryToken{" +
                "candidateBuckets=" + candidateBuckets +
                ", topK=" + topK +
                ", encryptionContext='" + encryptionContext + '\'' +
                '}';
    }
}
