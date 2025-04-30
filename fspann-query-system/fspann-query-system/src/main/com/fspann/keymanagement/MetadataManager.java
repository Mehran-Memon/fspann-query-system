package com.fspann.keymanagement;

import com.fspann.utils.PersistenceUtils;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetadataManager {

    private static final String METADATA_FILE_PATH = "metadata/metadata.ser";
    private static final String KEYS_FILE_PATH = "metadata/keys.ser";
    private Map<String, String> metadata;
    private KeyManager keyManager;

    public MetadataManager(KeyManager keyManager) {
        this.keyManager = keyManager;
        this.metadata = new HashMap<>();
        loadMetadata();
        loadKeys();  // Load keys after metadata is initialized
    }

    public void loadMetadata() {
        try {
            metadata = PersistenceUtils.loadObject(METADATA_FILE_PATH, Map.class);
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Metadata not found or error loading. Starting fresh.");
        }
    }

    public void saveMetadata() {
        try {
            PersistenceUtils.saveObject(metadata, METADATA_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadKeys() {
        try {
            Map<String, SecretKey> keys = PersistenceUtils.loadObject(KEYS_FILE_PATH, Map.class);

            if (keys != null && !keys.isEmpty()) {
                // If keys exist, initialize KeyManager with the existing keys
                this.keyManager = new KeyManager(keys, 1000); // Initialize with existing keys and a rotation interval
            } else {
                // If no keys found, initialize KeyManager with default settings
                this.keyManager = new KeyManager(1000);  // Default rotation interval
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Error loading keys or no keys found, initializing new KeyManager.");
            this.keyManager = new KeyManager(1000);  // Initialize with default rotation interval if no keys exist
        }
    }

    public void saveKeys() {
        try {
            PersistenceUtils.saveObject(keyManager.getKeyStore(), KEYS_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addMetadata(String key, String value) {
        metadata.put(key, value);
        saveMetadata();
    }

    public String getMetadata(String key) {
        return metadata.get(key);
    }
}
