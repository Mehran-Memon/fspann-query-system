package com.fspann.keymanagement;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fspann.index.SecureLSHIndex;

public class ReEncryptor implements Runnable {
    private final BlockingQueue<Integer> dirtyShards = new LinkedBlockingQueue<>();
    private final SecureLSHIndex index;
    private final KeyManager km;
    private volatile boolean running = true;

    public ReEncryptor(SecureLSHIndex index, KeyManager km) {
        this.index = index;
        this.km = km;
    }

    public void shutdown() { running = false; }

    @Override public void run() {
        while (running || !dirtyShards.isEmpty()) {
            try {
                Integer shard = dirtyShards.poll(1, TimeUnit.SECONDS);
                if (shard == null) continue;
                index.reEncryptShard(shard, km);   // batch size
            } catch (InterruptedException ignored) { } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
