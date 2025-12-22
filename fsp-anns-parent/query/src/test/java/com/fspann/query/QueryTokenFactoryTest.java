package com.fspann.query;

import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class QueryTokenFactoryTest {

    private QueryTokenFactory tokenFactory;
    private CryptoService mockCryptoService;
    private KeyLifeCycleService mockKeyService;
    private PartitionedIndexService mockPartitionedIndexService;
    private SystemConfig mockConfig;

    @BeforeEach
    void setUp() {
        mockCryptoService = mock(CryptoService.class);
        mockKeyService = mock(KeyLifeCycleService.class);
        mockPartitionedIndexService = mock(PartitionedIndexService.class);

        // Mocking PaperConfig and SystemConfig
        SystemConfig.PaperConfig mockPaperConfig = mock(SystemConfig.PaperConfig.class);
        when(mockPaperConfig.getTables()).thenReturn(3); // Return a valid value for getTables()
        when(mockConfig.getPaper()).thenReturn(mockPaperConfig); // Return mock PaperConfig in mockConfig

        mockConfig = mock(SystemConfig.class);

        // Initialize QueryTokenFactory with mocked dependencies
        tokenFactory = new QueryTokenFactory(mockCryptoService, mockKeyService, mockPartitionedIndexService, mockConfig, 8);
    }



    @Test
    void testCreateQueryToken() {
        double[] query = new double[]{1.0, 2.0, 3.0};
        int topK = 10;

        // Mock return values for the crypto and key service
        SecretKey mockKey = mock(SecretKey.class);  // Mock a SecretKey object
        when(mockCryptoService.encryptQuery(any(), any(), any())).thenReturn(new byte[]{1, 2, 3, 4});
        when(mockKeyService.getCurrentVersion()).thenReturn(new KeyVersion(1, mockKey));  // Use the mock SecretKey

        // Create token using QueryTokenFactory
        QueryToken token = tokenFactory.create(query, topK);

        // Assertions
        assertNotNull(token);
        assertEquals(topK, token.getTopK());
        assertEquals(8, token.getNumTables());  // Based on the config setup
        assertNotNull(token.getEncryptedQuery());
        assertNotNull(token.getIv());
    }


    @Test
    void testInvalidQuery() {
        double[] query = new double[]{}; // Empty query

        assertThrows(IllegalArgumentException.class, () -> tokenFactory.create(query, 10));
    }
}
