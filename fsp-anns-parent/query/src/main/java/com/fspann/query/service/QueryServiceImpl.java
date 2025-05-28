// com/fspann/query/service/QueryServiceImpl.java
package com.fspann.query.service;

import com.fspann.common.*;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.IndexService;
import java.util.List;
import java.util.stream.Collectors;

public class QueryServiceImpl implements QueryService {
    private final IndexService    indexService;
    private final CryptoService   cryptoService;
    private final KeyLifeCycleService keyService;

    public QueryServiceImpl(IndexService idx,
                            CryptoService crypto,
                            KeyLifeCycleService ks) {
        this.indexService  = idx;
        this.cryptoService = crypto;
        this.keyService    = ks;
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        // rotate if needed
        keyService.rotateIfNeeded();
        KeyVersion ver = keyService.getCurrentVersion();

        // 1) decrypt *the query* itself, using the IV from the token
        final double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(
                    token.getEncryptedQuery(),
                    token.getIv(),
                    ver.getSecretKey()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt query vector", e);
        }

        // 2) fetch encrypted candidates
        List<EncryptedPoint> cands = indexService.lookup(token);

        // 3) decrypt each point with its own IV + ciphertext
        return cands.stream()
        .map(pt -> {
            // decrypt *this point* under the same key version
            double[] ptVec = cryptoService.decryptFromPoint(pt, ver.getSecretKey());;
            double dist   = computeDistance(queryVec, ptVec);
            return new QueryResult(pt.getId(), dist);
            })
            .sorted()                      // sort by distance
            .limit(token.getTopK())        // top-K
            .collect(Collectors.toList());
    }

    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("Dim mismatch");
        double s=0;
        for (int i=0; i<a.length; i++) {
            double d = a[i]-b[i];
            s += d*d;
        }
        return Math.sqrt(s);
    }
}
