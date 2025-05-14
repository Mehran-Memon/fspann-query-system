package com.fspann.keymanagement;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fspann.utils.PersistenceUtils;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manages metadata and delegates key loading/saving to KeyManager.
 */
public class MetadataManager {
    private Map<String, String> metadata;
    private KeyManager keyManager;

    /**
     * Initialize with an existing KeyManager.
     */
    public MetadataManager(KeyManager keyManager) {
        this.keyManager = keyManager;
        this.metadata = new HashMap<>();
    }

    /**
     * Load metadata map from disk; start fresh on failure.
     */
    public void loadMetadata(String metadataFilePath) {
        try {
            Map<String, String> loaded = PersistenceUtils.loadObject(
                    metadataFilePath,
                    new TypeReference<Map<String, String>>() {}
            );

            if (loaded != null) {
                this.metadata = loaded;
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Metadata not found or error loading. Starting fresh.");
            this.metadata = new HashMap<>();
        }
    }

    /**
     * Persist metadata map to disk.
     */
    public void saveMetadata(String metadataFilePath) {
        try {
            PersistenceUtils.saveObject(metadata, metadataFilePath);
        } catch (IOException e) {
            System.err.println("Failed to save metadata: " + e.getMessage());
        }
    }

    /**
     * Load or reinitialize KeyManager based on existing key file.
     */
    public void loadKeys(String keysFilePath) throws IOException {
        KeyRotationPolicy policy = new KeyRotationPolicy(
                10000,
                TimeUnit.HOURS.toMillis(1),
                TimeUnit.SECONDS.toMillis(30),
                10000
        );

        try {
            // Use TypeReference to preserve generic type information
            Map<String, SecretKey> keys = PersistenceUtils.loadObject(
                    keysFilePath,
                    new TypeReference<HashMap<String, SecretKey>>() {}
            );

            if (keys != null && !keys.isEmpty()) {
                this.keyManager = new KeyManager(keysFilePath, policy);
                return;
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("No existing keys found or error loading; initializing new KeyManager.");
        }

        this.keyManager = new KeyManager(keysFilePath, policy);
    }

    /**
     * Persist the current KeyManager's key store to disk.
     */
    public void saveKeys(String keysFilePath) {
        try {
            PersistenceUtils.saveObject(keyManager.getKeyStore(), keysFilePath);
        } catch (IOException e) {
            System.err.println("Failed to save keys: " + e.getMessage());
        }
    }

    /**
     * Add a metadata entry and immediately persist it.
     */
    public void addMetadata(String key, String value, String metadataFilePath) {
        metadata.put(key, value);
        saveMetadata(metadataFilePath);
    }

    /**
     * Retrieve a metadata value by key.
     */
    public String getMetadata(String key) {
        return metadata.get(key);
    }


}