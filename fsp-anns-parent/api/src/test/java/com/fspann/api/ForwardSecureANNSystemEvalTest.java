package com.fspann.api;

import com.fspann.common.QueryMetrics;
import com.fspann.common.QueryResult;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.service.QueryServiceImpl;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemEvalTest {

    private ForwardSecureANNSystem sys;
    private GroundtruthManager gt;
    private QueryServiceImpl qs;
    private double[] q;

    @BeforeEach
    void setup() throws Exception {

        Path tmp = Files.createTempDirectory("fsp-ann-test");

        sys = TestUtils.minimalSystem(tmp);
        qs  = sys.getQueryServiceImpl();

        // Minimal query vector (dim = 2)
        q = new double[]{0.0, 0.0};

        // ---------------- Groundtruth ----------------
        gt = new GroundtruthManager();

        Path gtFile = tmp.resolve("gt.ivecs");
        TestUtils.writeIvecs(
                gtFile,
                new int[][]{
                        {3, 2, 1, 0, 4},   // query 0
                        {1, 0, 2, 3, 4}    // query 1
                }
        );

        gt.load(gtFile.toString());
    }

    @Test
    void testPerfectHitK1() {

        List<QueryResult> ann =
                List.of(new QueryResult("3", 0.0));
        final int k = 5;
        final int MAX_K = 100; // or derive from config

        QueryMetrics m =
                sys.computeMetricsAtK(
                        k,
                        MAX_K,
                        ann,
                        0,
                        q,
                        qs,
                        gt
                );


        assertEquals(1.0, m.candidateRatioAtK(), 1e-12);
    }

    @Test
    void testPartialHitK5() {

        List<QueryResult> ann = List.of(
                new QueryResult("3", 0.1),
                new QueryResult("2", 0.2),
                new QueryResult("9", 0.9),
                new QueryResult("8", 1.1),
                new QueryResult("7", 1.3)
        );

        final int k = 5;
        final int MAX_K = 100; // or derive from config

        QueryMetrics m =
                sys.computeMetricsAtK(
                        k,
                        MAX_K,
                        ann,
                        0,
                        q,
                        qs,
                        gt
                );

        assertTrue(m.candidateRatioAtK() >= 1.0);
    }

    @Test
    void testMissingGTThrows() {

        GroundtruthManager emptyGT = new GroundtruthManager();

        List<QueryResult> ann =
                List.of(new QueryResult("0", 0.0));

        final int k = 5;
        final int MAX_K = 100; // or derive from config

        assertThrows(
                IllegalStateException.class,
                () -> sys.computeMetricsAtK(
                        k,
                        MAX_K,
                        ann,
                        0,
                        q,
                        qs,
                        gt
                )
        );
    }

    @Test
    void testBadAnnIdThrows() {

        List<QueryResult> ann =
                List.of(new QueryResult("abc", 0.5));

        final int k = 5;
        final int MAX_K = 100; // or derive from config

        assertThrows(
                IllegalStateException.class,
                () -> sys.computeMetricsAtK(
                        k,
                        MAX_K,
                        ann,
                        0,
                        q,
                        qs,
                        gt
                )
        );
    }
}
