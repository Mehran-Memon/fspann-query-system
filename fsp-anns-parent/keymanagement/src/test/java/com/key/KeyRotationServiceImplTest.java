package com.key;

import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.common.KeyVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.SecretKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

class KeyRotationServiceImplTest {
    @Mock
    private KeyManager keyManager;

    @Mock
    private KeyRotationPolicy policy;

    @Mock
    private SecretKey mockKey;

    private KeyRotationServiceImpl service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(policy.getMaxOperations()).thenReturn(1000);
        when(policy.getMaxIntervalMillis()).thenReturn(7L * 24 * 60 * 60 * 1000L); // 7 days
        service = new KeyRotationServiceImpl(keyManager, policy);
    }

    @Test
    void testConcurrentRotation() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        IntStream.range(0, 1000).forEach(i -> executor.submit(service::incrementOperation));
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Updated KeyVersion call with dummy iv and encryptedQuery
        byte[] dummyIv = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte[] dummyEncryptedQuery = new byte[]{10, 20, 30, 40};
        KeyVersion newVersion = new KeyVersion(2, mockKey, dummyIv, dummyEncryptedQuery);

        when(keyManager.rotateKey()).thenReturn(newVersion);

        synchronized (service) {
            service.rotateIfNeeded();
            verify(keyManager).rotateKey();
        }
    }
}
