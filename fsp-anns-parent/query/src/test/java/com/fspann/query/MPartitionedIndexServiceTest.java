package com.fspann.query;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.KeyRotationServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class MPartitionedIndexServiceTest {

    private PartitionedIndexService indexService;
    private RocksDBMetadataManager mockMetadataManager;
    private SystemConfig mockConfig;
    private KeyRotationServiceImpl mockKeyService;
    private AesGcmCryptoService mockCryptoService;

    @BeforeEach
    void setUp() {
        mockMetadataManager = mock(RocksDBMetadataManager.class);
        mockConfig = mock(SystemConfig.class);
        mockKeyService = mock(KeyRotationServiceImpl.class);
        mockCryptoService = mock(AesGcmCryptoService.class);

        indexService = new PartitionedIndexService(mockMetadataManager, mockConfig, mockKeyService, mockCryptoService);
    }

    @Test
    void testInsertEncryptedPoint() throws IOException {
        // Setup test data
        String id = "test-id";
        double[] vector = new double[]{1.0, 2.0, 3.0};

        // Mock encryption process
        EncryptedPoint mockEncryptedPoint = mock(EncryptedPoint.class);
        when(mockCryptoService.encrypt(anyString(), any(), any())).thenReturn(mockEncryptedPoint);

        // Call insert method
        indexService.insert(id, vector);

        // Verify interactions
        verify(mockMetadataManager, times(1)).saveEncryptedPoint(mockEncryptedPoint);
    }


    @Test
    void testInsertInvalidVector() {
        String id = "test-id";
        double[] invalidVector = null; // Invalid vector

        assertThrows(IllegalArgumentException.class, () -> indexService.insert(id, invalidVector));
    }
}

