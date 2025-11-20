package com.index;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.index.service.SecureLSHIndexService;

import org.junit.jupiter.api.*;

import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Ensures the probe-shards system property correctly influences
 * bucket selection & widening logic used by SecureLSHIndexService.
 */
class SecureLSHIndexProbeShardsTest {

    private CryptoService crypto;
    private KeyLifeCycleService keySvc;
    private SecureLSHIndexService svc;

    private PartitionedIndexService paper;

    @BeforeEach
    void setup() {
        crypto = mock(CryptoService.class);
        keySvc = mock(KeyLifeCycleService.class);

        // Stable mock key
        when(keySvc.getCurrentVersion())
                .thenReturn(new KeyVersion(1, new SecretKeySpec(new byte[32], "AES")));

        // Pure paper-index (no real RocksDB, just mocks)
        paper = new PartitionedIndexService(
                /*m*/ 12,
                /*lambda*/ 6,
                /*div*/ 4,
                /*seed*/ 1234L,
                /*buildThreshold*/ 1,
                /*maxCandidates*/ -1
        );

        RocksDBMetadataManager meta = mock(RocksDBMetadataManager.class);
        EncryptedPointBuffer buffer = mock(EncryptedPointBuffer.class);

        when(meta.getPointsBaseDir())
                .thenReturn(System.getProperty("java.io.tmpdir") + "/points");

        // SecureLSHIndexService in paper-mode only with mocks
        svc = new SecureLSHIndexService(
                crypto,
                keySvc,
                meta,
                paper,
                buffer
        );
    }

    @AfterEach
    void cleanup() {
        System.clearProperty("probe.shards");
    }

    private static EncryptedPoint pt(String id, int dim) {
        return new EncryptedPoint(
                id,
                0,
                new byte[12],
                new byte[16],
                1,
                dim,
                Collections.emptyList()
        );
    }

    @Test
    void probeShards_default_is_used_when_property_missing() {
        double[] vec = {1, 1, 1, 1};
        paper.insert(pt("p1", 4), vec);

        QueryToken token = new QueryToken(
                List.of(),
                paper.code(vec),
                new byte[12],
                new byte[16],
                10, 1,
                "epoch_1_dim_4",
                4, 1
        );

        // No override → PartitionedIndexService should behave normally
        var out = svc.lookup(token);
        assertFalse(out.isEmpty(), "Lookup should work with default probing");
    }

    @Test
    void probeShards_property_overrides_server_logic() {
        double[] vec = {0.2, -0.1, 0.4, 0.8};
        paper.insert(pt("p1", 4), vec);

        // Force wider probing on the server side
        System.setProperty("probe.shards", "999");

        QueryToken token = new QueryToken(
                List.of(),
                paper.code(vec),
                new byte[12],
                new byte[16],
                10, 1,
                "epoch_1_dim_4",
                4, 1
        );

        var out = svc.lookup(token);
        assertFalse(out.isEmpty());
        assertTrue(out.size() >= 1, "Probe widening must not reduce candidates");
    }

    @Test
    void repeatedProbeWidening_never_reduces_candidates() {
        double[] vec = {1, 0, 1, 0};
        paper.insert(pt("p1", 4), vec);
        paper.insert(pt("p2", 4), vec);

        QueryToken token = new QueryToken(
                List.of(),
                paper.code(vec),
                new byte[12],
                new byte[16],
                10, 1,
                "epoch_1_dim_4",
                4, 1
        );

        System.setProperty("probe.shards", "1");
        int small = svc.lookup(token).size();

        System.setProperty("probe.shards", "500");
        int large = svc.lookup(token).size();

        assertTrue(large >= small,
                "Wider probe.shards must never reduce candidate count");
    }

    @Test
    void removingProbeShardsProperty_restoresDefaultBehavior() {
        double[] vec = {0.9, 0.1, 0.9, 0.2};
        paper.insert(pt("p1", 4), vec);

        QueryToken token = new QueryToken(
                List.of(),
                paper.code(vec),
                new byte[12],
                new byte[16],
                10, 1,
                "epoch_1_dim_4",
                4, 1
        );

        // First, widen
        System.setProperty("probe.shards", "500");
        int widened = svc.lookup(token).size();

        // Remove override
        System.clearProperty("probe.shards");
        int restored = svc.lookup(token).size();

        assertTrue(restored <= widened,
                "Removing property should return to default narrower probing");
    }
}
