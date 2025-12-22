package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import static org.mockito.Mockito.*;

import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class MQueryServiceImplTest {

    private QueryServiceImpl queryService;
    private PartitionedIndexService mockIndexService;
    private CryptoService mockCryptoService;
    private KeyLifeCycleService mockKeyService;
    private QueryTokenFactory mockTokenFactory;

    @BeforeEach
    void setUp() {
        // Mocking necessary dependencies
        mockIndexService = mock(PartitionedIndexService.class);
        mockCryptoService = mock(CryptoService.class);
        mockKeyService = mock(KeyLifeCycleService.class);
        mockTokenFactory = mock(QueryTokenFactory.class);

        // Mock RocksDBMetadataManager to return a valid StorageMetrics
        RocksDBMetadataManager mockMetadataManager = mock(RocksDBMetadataManager.class);
        StorageMetrics mockStorageMetrics = mock(StorageMetrics.class);
        when(mockMetadataManager.getStorageMetrics()).thenReturn(mockStorageMetrics);

        // Initialize the PartitionedIndexService with the mock metadata manager
        PartitionedIndexService mockIndexService = new PartitionedIndexService(mockMetadataManager, mock(SystemConfig.class), mock(KeyRotationServiceImpl.class), mock(AesGcmCryptoService.class));

        queryService = new QueryServiceImpl(mockIndexService, mockCryptoService, mockKeyService, mockTokenFactory, mock(SystemConfig.class));
    }


    @Test
    void testSearchWithValidQuery() {
        // Setup the query and the mock behavior for the TokenFactory
        double[] query = new double[]{1.0, 2.0, 3.0};
        QueryToken mockToken = mock(QueryToken.class);
        when(mockTokenFactory.create(query, 10)).thenReturn(mockToken);

        // Create a list of mock results that you expect from the 'lookup' method
        List<QueryResult> mockResults = List.of(
                new QueryResult("id1", 0.5),
                new QueryResult("id2", 0.8)
        );

        // Mock the 'search' method to return the mock results when called
        when(queryService.search(mockToken)).thenReturn(mockResults);

        // Mocking KeyVersion and KeyLifeCycleService for valid `kv`
        SecretKey mockKey = mock(SecretKey.class);  // Mock SecretKey
        KeyVersion mockKeyVersion = mock(KeyVersion.class);
        when(mockKeyVersion.getKey()).thenReturn(mockKey);
        when(mockKeyService.getVersion(anyInt())).thenReturn(mockKeyVersion);  // Ensure that getVersion returns a valid KeyVersion

        // Perform the search using the QueryService
        List<QueryResult> results = queryService.search(mockToken);

        // Assertions
        assertNotNull(results);
        assertEquals(2, results.size());
        assertEquals("id1", results.get(0).getId());
        assertEquals(0.5, results.get(0).getDistance());
    }



    @Test
    void testSearchWithEmptyResults() {
        // Setup the query and the mock behavior for the TokenFactory
        double[] query = new double[]{1.0, 2.0, 3.0};
        QueryToken mockToken = mock(QueryToken.class);
        when(mockTokenFactory.create(query, 10)).thenReturn(mockToken);

        // Mock the 'search' method to return empty results
        when(queryService.search(mockToken)).thenReturn(List.of());

        // Perform the search using the QueryService
        List<QueryResult> results = queryService.search(mockToken);

        // Assertions
        assertTrue(results.isEmpty());
    }
}
