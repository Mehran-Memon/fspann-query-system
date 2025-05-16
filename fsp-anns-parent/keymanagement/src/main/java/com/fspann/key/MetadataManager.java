package com.fspann.key;

import com.fspann.common.PersistenceUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple manager for persisting string metadata alongside keys.
 */
public class MetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    private Map<String, String> metadata = new HashMap<>();

    public void load(String path) {
        try {
            // specify the generic type so loadObject can deserialize correctly
            Map<String, String> loaded = PersistenceUtils.loadObject(
                    path,
                    new TypeReference<Map<String, String>>() {}
            );
            metadata = (loaded != null ? new HashMap<>(loaded) : new HashMap<>());
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Failed to load metadata from {}, starting fresh", path, e);
            metadata = new HashMap<>();
        }
    }

    public void save(String path) {
        try {
            // HashMap is Serializable, so this works
            PersistenceUtils.saveObject((java.io.Serializable)metadata, path);
        } catch (IOException e) {
            logger.error("Failed to save metadata to {}", path, e);
        }
    }

    public void put(String key, String value) {
        metadata.put(key, value);
    }

    public String get(String key) {
        return metadata.get(key);
    }

    public Map<String, String> all() {
        return Collections.unmodifiableMap(metadata);
    }
}
