package com.fspann.common;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for serializing and deserializing objects to disk using Java serialization.
 */
public class PersistenceUtils {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);

    /** Serializes any Serializable object to the given file path. */
    public static <T extends java.io.Serializable> void saveObject(T obj, String path)
            throws IOException {
        Path p = Paths.get(path);
        if (p.getParent() != null) Files.createDirectories(p.getParent());
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
            oos.writeObject(obj);
            logger.debug("Saved object to {}", path);
        }
    }

    /** Reads an object from the given file path, expecting it to be Serializable. */
    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, TypeReference<T> typeRef) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
            T obj = (T) ois.readObject();
            logger.debug("Loaded object from {}", path);
            return obj;
        } catch (IOException e) {
            logger.error("Failed to load object from {}", path, e);
            throw e;
        } catch (ClassNotFoundException e) {
            logger.error("Class not found while loading object from {}", path, e);
            throw e;
        }
    }
}