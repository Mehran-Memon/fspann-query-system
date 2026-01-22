package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
class SuperSystemLifecycleIT extends BaseUnifiedIT {

    @Test
    @DisplayName("Full lifecycle: index → query → persist → restore")
    void fullLifecycleWorks() throws IOException, InterruptedException {
        indexClusteredData(50);

        double[] query = new double[DIM];
        Arrays.fill(query, 5.0);

        QueryToken tok = system.createToken(query, 10, DIM);
        List<QueryResult> results = system.getQueryServiceImpl().search(tok);

        // FIXED: Correct assertion - should NOT be empty
        assertTrue(results != null && !results.isEmpty(),
                "Should return results");
        assertTrue(results.size() <= 10,
                "Should not exceed K");

        int touched = system.getIndexService().getLastTouchedCount();
        assertTrue(touched >= 0, "Touched count should be non-negative");

        // Flush before shutdown
        system.flushAll();
        metadata.flush();
        Thread.sleep(100);

        system.shutdown();
        metadata.close();

        // Restore metadata
        metadata = RocksDBMetadataManager.create(
                metaDir.toString(), ptsDir.toString()
        );

        // Rebuild system
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

        // Restore index from disk
        int currentVersion = keyService.getCurrentVersion().getVersion();
        int restored = system.restoreIndexFromDisk(currentVersion);

        assertTrue(restored > 0, "Should restore at least one vector");

        // Verify system still works after restore
        assertDoesNotThrow(() -> {
            system.finalizeForSearch();
            QueryToken tok2 = system.createToken(query, 10, DIM);
            List<QueryResult> results2 = system.getQueryServiceImpl().search(tok2);
            assertNotNull(results2, "Should return results after restore");
        });
    }
}