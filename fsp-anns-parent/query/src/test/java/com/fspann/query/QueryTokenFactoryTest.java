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
import java.util.BitSet;

import static org.mockito.Mockito.*;
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

        // Create mockConfig FIRST
        mockConfig = mock(SystemConfig.class);

        // Create a REAL PaperConfig with concrete values (not a mock)
        // because QueryTokenFactory accesses FIELDS directly (pc.divisions, pc.seed)
        // and Mockito cannot mock field access
        SystemConfig.PaperConfig realPaperConfig = new SystemConfig.PaperConfig() {
            @Override
            public int getTables() {
                return 3; // Number of tables (L)
            }
        };
        realPaperConfig.divisions = 8;  // Field access
        realPaperConfig.seed = 42L;     // Field access
        realPaperConfig.lambda = 10;    // Field access
        realPaperConfig.m = 5;          // Field access

        // Mock the getter methods as well (for other code paths)
        when(mockConfig.getPaper()).thenReturn(realPaperConfig);

        // Mock the partition.code() method to return valid BitSets matching divisions
        // But return null for empty vectors (to simulate real behavior)
        when(mockPartitionedIndexService.code(any(double[].class), anyLong())).thenAnswer(invocation -> {
            double[] vector = invocation.getArgument(0);

            // Return null for empty vectors (simulates real partition.code() failure)
            if (vector == null || vector.length == 0) {
                return null;
            }

            // Return valid codes for non-empty vectors
            BitSet[] mockCodes = new BitSet[8]; // MUST match pc.divisions = 8
            for (int i = 0; i < 8; i++) {
                mockCodes[i] = new BitSet();
                mockCodes[i].set(i); // Just set some bits
            }
            return mockCodes;
        });

        // Initialize QueryTokenFactory with mocked dependencies
        tokenFactory = new QueryTokenFactory(mockCryptoService, mockKeyService, mockPartitionedIndexService, mockConfig, 8);
    }

    @Test
    void testCreateQueryToken() {
        double[] query = new double[]{1.0, 2.0, 3.0};
        int topK = 10;

        // Mock return values for the crypto and key service
        SecretKey mockKey = mock(SecretKey.class);
        when(mockCryptoService.encryptQuery(any(), any(), any())).thenReturn(new byte[]{1, 2, 3, 4});
        when(mockKeyService.getCurrentVersion()).thenReturn(new KeyVersion(1, mockKey));

        // Create token using QueryTokenFactory
        QueryToken token = tokenFactory.create(query, topK);

        // Assertions
        assertNotNull(token);
        assertEquals(topK, token.getTopK());
        assertEquals(3, token.getNumTables());  // Based on realPaperConfig.getTables() = 3
        assertNotNull(token.getEncryptedQuery());
        assertNotNull(token.getIv());
        assertNotNull(token.getCodesByTable());
        assertEquals(3, token.getCodesByTable().length); // Should have 3 tables worth of codes
    }

    @Test
    void testInvalidQuery() {
        double[] query = new double[]{}; // Empty query

        // Need to mock KeyVersion because code encrypts query BEFORE partition.code() validation
        SecretKey mockKey = mock(SecretKey.class);
        when(mockKeyService.getCurrentVersion()).thenReturn(new KeyVersion(1, mockKey));
        when(mockCryptoService.encryptQuery(any(), any(), any())).thenReturn(new byte[]{1, 2, 3, 4});

        // Note: Empty vector doesn't throw IllegalArgumentException - it goes through
        // the code until partition.code() fails with IllegalStateException
        // because the validation only checks topK, not vector length
        assertThrows(IllegalStateException.class, () -> tokenFactory.create(query, 10));
    }

    @Test
    void testInvalidTopK() {
        double[] query = new double[]{1.0, 2.0, 3.0};

        // topK <= 0 should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> tokenFactory.create(query, 0));
        assertThrows(IllegalArgumentException.class, () -> tokenFactory.create(query, -1));
    }

    @Test
    void testNullVector() {
        // Null vector should throw NullPointerException
        assertThrows(NullPointerException.class, () -> tokenFactory.create(null, 10));
    }
}