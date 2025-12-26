package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuperSystemLifecycleIT extends BaseUnifiedIT {

    @Test
    @DisplayName("Full lifecycle: index → query → persist → restore")
    void fullLifecycleWorks() throws IOException {

        indexClusteredData(50);

        double[] query = new double[DIM];
        Arrays.fill(query, 5.0);

        QueryToken tok = system.createToken(query, 10, DIM);
        List<QueryResult> results = system.getQueryServiceImpl().search(tok);

        assertFalse(results.isEmpty());
        assertTrue(results.size() <= 10);

        int touched = system.getIndexService().getLastTouchedCount();
        assertTrue(touched > 0);

        // ---- restart system (persistence)
        system.shutdown();
        metadata.close();

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(), ptsDir.toString());

        system = new ForwardSecureANNSystem(
                cfgFile.toString(),
                seedFile.toString(),
                ksFile.toString(),
                List.of(DIM),
                root,
                false,
                metadata,
                crypto,
                32
        );

        int restored = system.restoreIndexFromDisk(
                keyService.getCurrentVersion().getVersion());

        assertTrue(restored > 0);
    }
}

