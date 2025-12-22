package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

public class MQueryServiceImplTest {

    private QueryServiceImpl queryService;
    private PartitionedIndexService mockIndexService;
    private CryptoService mockCryptoService;
    private KeyLifeCycleService mockKeyService;
    private QueryTokenFactory mockTokenFactory;
    private SystemConfig mockConfig;

    @BeforeEach
    void setUp() {
        // Mock all dependencies - don't create real instances
        mockIndexService = mock(PartitionedIndexService.class);
        mockCryptoService = mock(CryptoService.class);
        mockKeyService = mock(KeyLifeCycleService.class);
        mockTokenFactory = mock(QueryTokenFactory.class);
        mockConfig = mock(SystemConfig.class);

        // Mock SystemConfig stabilization and runtime configs
        SystemConfig.StabilizationConfig mockStabilizationConfig = mock(SystemConfig.StabilizationConfig.class);
        when(mockStabilizationConfig.isEnabled()).thenReturn(false); // Disable stabilization for simpler tests
        when(mockConfig.getStabilization()).thenReturn(mockStabilizationConfig);

        SystemConfig.RuntimeConfig mockRuntimeConfig = mock(SystemConfig.RuntimeConfig.class);
        when(mockRuntimeConfig.getMaxRefinementFactor()).thenReturn(5);
        when(mockConfig.getRuntime()).thenReturn(mockRuntimeConfig);

        // Initialize QueryServiceImpl with mocked dependencies
        queryService = new QueryServiceImpl(
                mockIndexService,
                mockCryptoService,
                mockKeyService,
                mockTokenFactory,
                mockConfig
        );
    }

    @Test
    void testSearchWithValidQuery() {
        // Setup mock QueryToken
        QueryToken mockToken = mock(QueryToken.class);
        when(mockToken.getVersion()).thenReturn(1);
        when(mockToken.getTopK()).thenReturn(10);
        when(mockToken.getEncryptedQuery()).thenReturn(new byte[]{1, 2, 3});
        when(mockToken.getIv()).thenReturn(new byte[]{4, 5, 6});

        // Mock KeyVersion and SecretKey - MUST BE SET UP BEFORE search() is called
        SecretKey mockKey = mock(SecretKey.class);
        KeyVersion mockKeyVersion = new KeyVersion(1, mockKey);
        when(mockKeyService.getVersion(1)).thenReturn(mockKeyVersion);
        when(mockKeyService.getCurrentVersion()).thenReturn(mockKeyVersion);

        // Mock decrypted query vector
        double[] queryVector = new double[]{1.0, 2.0, 3.0};
        when(mockCryptoService.decryptQuery(any(byte[].class), any(byte[].class), any(SecretKey.class)))
                .thenReturn(queryVector);

        // Mock candidate IDs from index
        List<String> candidateIds = List.of("id1", "id2");
        when(mockIndexService.lookupCandidateIds(mockToken)).thenReturn(candidateIds);

        // Mock encrypted points
        EncryptedPoint mockPoint1 = mock(EncryptedPoint.class);
        when(mockPoint1.getId()).thenReturn("id1");
        when(mockPoint1.getKeyVersion()).thenReturn(1);

        EncryptedPoint mockPoint2 = mock(EncryptedPoint.class);
        when(mockPoint2.getId()).thenReturn("id2");
        when(mockPoint2.getKeyVersion()).thenReturn(1);

        when(mockIndexService.loadPointIfActive("id1")).thenReturn(mockPoint1);
        when(mockIndexService.loadPointIfActive("id2")).thenReturn(mockPoint2);

        // Mock decryption of candidate vectors
        double[] vector1 = new double[]{1.1, 2.1, 3.1};
        double[] vector2 = new double[]{1.2, 2.2, 3.2};
        when(mockCryptoService.decryptFromPoint(mockPoint1, mockKey)).thenReturn(vector1);
        when(mockCryptoService.decryptFromPoint(mockPoint2, mockKey)).thenReturn(vector2);

        // Perform the search
        List<QueryResult> results = queryService.search(mockToken);

        // Assertions
        assertNotNull(results);
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(r -> r.getId().equals("id1")));
        assertTrue(results.stream().anyMatch(r -> r.getId().equals("id2")));
    }

    @Test
    void testSearchWithEmptyResults() {
        // Setup mock QueryToken
        QueryToken mockToken = mock(QueryToken.class);
        when(mockToken.getVersion()).thenReturn(1);
        when(mockToken.getTopK()).thenReturn(10);
        when(mockToken.getEncryptedQuery()).thenReturn(new byte[]{1, 2, 3});
        when(mockToken.getIv()).thenReturn(new byte[]{4, 5, 6});

        // Mock KeyVersion
        SecretKey mockKey = mock(SecretKey.class);
        KeyVersion mockKeyVersion = new KeyVersion(1, mockKey);
        when(mockKeyService.getVersion(1)).thenReturn(mockKeyVersion);
        when(mockKeyService.getCurrentVersion()).thenReturn(mockKeyVersion);

        // Mock decrypted query vector
        double[] queryVector = new double[]{1.0, 2.0, 3.0};
        when(mockCryptoService.decryptQuery(any(byte[].class), any(byte[].class), any(SecretKey.class)))
                .thenReturn(queryVector);

        // Mock empty candidate list
        when(mockIndexService.lookupCandidateIds(mockToken)).thenReturn(Collections.emptyList());

        // Perform the search
        List<QueryResult> results = queryService.search(mockToken);

        // Assertions
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
}