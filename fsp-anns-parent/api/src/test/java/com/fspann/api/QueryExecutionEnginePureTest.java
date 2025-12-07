package com.fspann.api;

import com.fspann.common.Profiler;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.service.QueryServiceImpl;

import org.junit.jupiter.api.*;
import java.nio.file.Path;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class QueryExecutionEnginePureTest {

    @Test
    void engine_callsSysRunKAdaptiveProbeOnly_whenEnabled() {
        ForwardSecureANNSystem sys = mock(ForwardSecureANNSystem.class);
        QueryServiceImpl qs = mock(QueryServiceImpl.class);
        Profiler profiler = mock(Profiler.class);

        when(sys.kAdaptiveProbeEnabled()).thenReturn(true);
        when(sys.getQueryServiceImpl()).thenReturn(qs);
        when(sys.getFactoryForDim(2)).thenThrow(new RuntimeException("stop here for unit test"));

        QueryExecutionEngine engine =
                new QueryExecutionEngine(sys, profiler, new int[]{1,3});

        List<double[]> queries = List.of(new double[]{0.1,0.1});

        assertThrows(RuntimeException.class, () ->
                engine.evalBatch(queries, 2, mock(GroundtruthManager.class), Path.of("."), true)
        );

        verify(sys, times(1))
                .runKAdaptiveProbeOnly(eq(0), any(double[].class), eq(2), eq(qs));

        verify(sys, times(1)).resetProbeShards();
    }
}
