package com.fspann.keymanagement;

import com.fspann.utils.PersistenceUtils;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetadataManager {
    private Map<String, String> metadata;
    private KeyManager keyManager;

    // Constructor accepts keyManager and metadata/keys file paths
    public MetadataManager(KeyManager keyManager) {
        this.keyManager = keyManager;
        this.metadata = new HashMap<>();
    }

    // Load metadata from the specified file path
    public void loadMetadata(String metadataFilePath) {
        try {
            metadata = PersistenceUtils.loadObject(metadataFilePath, Map.class);
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Metadata not found or error loading. Starting fresh.");
        }
    }

    // Save metadata to the specified file path
    public void saveMetadata(String metadataFilePath) {
        try {
            PersistenceUtils.saveObject(metadata, metadataFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Load keys from the specified file path and initialize KeyManager
    public void loadKeys(String keysFilePath) throws IOException, ClassNotFoundException {
        try {
            Map<String, SecretKey> keys = PersistenceUtils.loadObject(keysFilePath, Map.class);
            if (keys != null && !keys.isEmpty()) {
                this.keyManager = new KeyManager(keysFilePath, 1000); // Initialize with existing keys
            } else {
                this.keyManager = new KeyManager(keysFilePath, 1000); // Initialize with default settings
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Error loading keys or no keys found, initializing new KeyManager.");
            this.keyManager = new KeyManager(keysFilePath, 1000);  // Default rotation interval if no keys exist
        }
    }

    // Save keys to the specified file path
    public void saveKeys(String keysFilePath) {
        try {
            PersistenceUtils.saveObject(keyManager.getKeyStore(), keysFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Add metadata and immediately save it
    public void addMetadata(String key, String value, String metadataFilePath) {
        metadata.put(key, value);
        saveMetadata(metadataFilePath);
    }

    // Retrieve metadata value by key
    public String getMetadata(String key) {
        return metadata.get(key);
    }
}
