package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.key.KeyUsageTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

public class MPartitionedIndexServiceTest {

    private PartitionedIndexService indexService;
    private RocksDBMetadataManager mockMetadataManager;
    private SystemConfig mockConfig;
    private KeyRotationServiceImpl mockKeyRotationService;
    private AesGcmCryptoService mockCryptoService;

    @BeforeEach
    void setUp() {
        // Mock dependencies
        mockMetadataManager = mock(RocksDBMetadataManager.class);
        mockConfig = mock(SystemConfig.class);
        mockKeyRotationService = mock(KeyRotationServiceImpl.class);

        KeyManager mockKeyManager = mock(KeyManager.class);
        KeyUsageTracker mockTracker = mock(KeyUsageTracker.class);

        when(mockKeyRotationService.getKeyManager()).thenReturn(mockKeyManager);
        when(mockKeyManager.getUsageTracker()).thenReturn(mockTracker);

        when(mockTracker.getVectorCount(anyInt())).thenReturn(0);
        when(mockTracker.isSafeToDelete(anyInt())).thenReturn(false);

        // Use spy instead of mock to preserve default interface methods
        mockCryptoService = mock(AesGcmCryptoService.class);

        // CRITICAL: Mock StorageMetrics to prevent IllegalStateException
        StorageMetrics mockStorageMetrics = mock(StorageMetrics.class);

        // Setup BEFORE calling getStorageMetrics()
        when(mockMetadataManager.getStorageMetrics()).thenReturn(mockStorageMetrics);

        // Mock StorageSnapshot for the metrics
        StorageMetrics.StorageSnapshot mockSnapshot = new StorageMetrics.StorageSnapshot(
                0L, 0L, 0L,
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>()
        );
        when(mockStorageMetrics.getSnapshot()).thenReturn(mockSnapshot);
        when(mockStorageMetrics.getSummary()).thenReturn("mock-storage-summary");

        // Create a REAL PaperConfig because PartitionedIndexService accesses fields directly
        SystemConfig.PaperConfig realPaperConfig = new SystemConfig.PaperConfig() {
            @Override
            public int getTables() { return 3; }
        };
        realPaperConfig.divisions = 8;
        realPaperConfig.seed = 42L;
        realPaperConfig.lambda = 10;
        realPaperConfig.m = 5;

        when(mockConfig.getPaper()).thenReturn(realPaperConfig);

        // Mock Runtime config
        SystemConfig.RuntimeConfig mockRuntimeConfig = mock(SystemConfig.RuntimeConfig.class);
        when(mockRuntimeConfig.getMaxCandidateFactor()).thenReturn(5);
        when(mockRuntimeConfig.getMaxRelaxationDepth()).thenReturn(3);
        when(mockRuntimeConfig.getEarlyStopCandidates()).thenReturn(1000);
        when(mockConfig.getRuntime()).thenReturn(mockRuntimeConfig);

        // Initialize PartitionedIndexService with mocked dependencies
        indexService = new PartitionedIndexService(
                mockMetadataManager,
                mockConfig,
                mockKeyRotationService,
                mockCryptoService
        );
    }

    @Test
    void testInsertWithStringId() throws Exception {

        String vectorId = "test-vector-1";
        double[] vector = new double[]{1.0, 2.0, 3.0};

        EncryptedPoint mockEncryptedPoint = mock(EncryptedPoint.class);
        when(mockEncryptedPoint.getId()).thenReturn(vectorId);
        when(mockEncryptedPoint.getVersion()).thenReturn(1);
        when(mockEncryptedPoint.getKeyVersion()).thenReturn(1);
        when(mockEncryptedPoint.getShardId()).thenReturn(0);
        when(mockEncryptedPoint.getVectorLength()).thenReturn(3);

        SecretKey mockKey = mock(SecretKey.class);
        KeyVersion mockKeyVersion = new KeyVersion(1, mockKey);
        when(mockKeyRotationService.getCurrentVersion()).thenReturn(mockKeyVersion);

        when(mockCryptoService.encrypt(
                eq(vectorId),
                eq(vector),
                any(KeyVersion.class)
        )).thenReturn(mockEncryptedPoint);

        doNothing().when(mockMetadataManager).saveEncryptedPoint(any());

        indexService.insert(vectorId, vector);

        verify(mockCryptoService).encrypt(eq(vectorId), eq(vector), any(KeyVersion.class));
        verify(mockMetadataManager).saveEncryptedPoint(mockEncryptedPoint);
    }

    @Test
    void testInsertWithEncryptedPoint() throws Exception {
        // Test the insert(EncryptedPoint, double[]) method directly
        double[] vector = new double[]{1.0, 2.0, 3.0};

        // Create mock encrypted point
        EncryptedPoint mockEncryptedPoint = mock(EncryptedPoint.class);
        when(mockEncryptedPoint.getId()).thenReturn("test-vector-2");
        when(mockEncryptedPoint.getVersion()).thenReturn(1);
        when(mockEncryptedPoint.getKeyVersion()).thenReturn(1);
        when(mockEncryptedPoint.getShardId()).thenReturn(0);
        when(mockEncryptedPoint.getVectorLength()).thenReturn(3);

        // Mock metadata save
        doNothing().when(mockMetadataManager).saveEncryptedPoint(any(EncryptedPoint.class));

        // Call insert with EncryptedPoint
        indexService.insert(mockEncryptedPoint, vector);

        // Verify metadata save was called
        verify(mockMetadataManager).saveEncryptedPoint(mockEncryptedPoint);
    }

    @Test
    void testInsertInvalidVector() {
        // Test with null ID - should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            indexService.insert((String) null, new double[]{1.0, 2.0});
        });

        // Test with null vector - should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            indexService.insert("test-id", (double[]) null);
        });
    }

    @Test
    void testInsertWithEncryptedPointNullInputs() {
        // Test insert(EncryptedPoint, double[]) with null inputs
        double[] vector = new double[]{1.0, 2.0, 3.0};

        assertThrows(NullPointerException.class, () -> {
            indexService.insert((EncryptedPoint) null, vector);
        });

        EncryptedPoint mockPoint = mock(EncryptedPoint.class);
        assertThrows(NullPointerException.class, () -> {
            indexService.insert(mockPoint, (double[]) null);
        });
    }
}