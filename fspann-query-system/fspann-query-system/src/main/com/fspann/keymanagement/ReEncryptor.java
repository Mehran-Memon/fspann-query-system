package com.fspann.keymanagement;

import com.fspann.ForwardSecureANNSystem;
import com.fspann.index.DimensionContext;
import com.fspann.keymanagement.KeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class ReEncryptor implements Runnable {
    private final BlockingQueue<Integer> dirtyShards = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Integer, DimensionContext> index; // Change SecureLSHIndex to ConcurrentHashMap
    private final KeyManager km;
    private volatile boolean running = true;
    private static final Logger logger = LoggerFactory.getLogger(ReEncryptor.class);

    // Modify constructor to accept ConcurrentHashMap
    public ReEncryptor(ConcurrentHashMap<Integer, DimensionContext> index, KeyManager km) {
        this.index = index;
        this.km = km;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        while (running || !dirtyShards.isEmpty()) {
            try {
                Integer shard = dirtyShards.poll(1, TimeUnit.SECONDS);
                if (shard == null) continue;
                // Here we call reEncryptShard with the new data type
                reEncryptShard(shard, km);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void reEncryptShard(int shardId, KeyManager km) {
        DimensionContext context = index.get(shardId);
        if (context != null) {
            try {
                // Perform re-encryption using the KeyManager
                context.reEncrypt(km, km.getPreviousKey()); // Use old key to decrypt and new key to encrypt
            } catch (Exception e) {
                logger.error("Failed to re-encrypt shard {}: {}", shardId, e.getMessage());
            }
        }
    }

}
