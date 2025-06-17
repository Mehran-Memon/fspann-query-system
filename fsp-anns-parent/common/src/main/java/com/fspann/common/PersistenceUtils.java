package com.fspann.common;

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;

/**
 * Utility methods for serializing and deserializing objects to disk using Java serialization.
 */
public class PersistenceUtils {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);

    /** Serializes any Serializable object to the given file path. */
    public static <T extends Serializable> void saveObject(T obj, String path) throws IOException {
        Path p = Paths.get(path);
        if (p.getParent() != null) Files.createDirectories(p.getParent());

        try (OutputStream fos = Files.newOutputStream(p);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(obj);
            logger.debug("✅ Saved object to {}", path);
        }
    }

    /** Reads a serialized object from file path and casts it to the expected type. */
    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, TypeReference<T> typeRef) throws IOException, ClassNotFoundException {
        Path p = Paths.get(path);
        try (InputStream fis = Files.newInputStream(p);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            T obj = (T) ois.readObject();
            logger.debug("✅ Loaded object from {}", path);
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            logger.error("❌ Failed to load object from {}: {}", path, e.toString());
            throw e;
        }
    }
}
