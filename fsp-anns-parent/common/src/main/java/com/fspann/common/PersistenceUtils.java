package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class PersistenceUtils {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);

    /**
     * Saves a serializable object to a file.
     *
     * @param object     The object to save
     * @param filePath   The file path to save to
     * @param baseDir    The base directory for relative path resolution
     * @throws IOException If an I/O error occurs
     */
    public static void saveObject(Serializable object, String filePath, String baseDir) throws IOException {
        Objects.requireNonNull(object, "Object to save cannot be null");
        Objects.requireNonNull(filePath, "File path cannot be null");
        Objects.requireNonNull(baseDir, "Base directory cannot be null");

        Path basePath = Paths.get(baseDir);
        Path targetPath = Paths.get(filePath);
        if (!targetPath.isAbsolute()) {
            targetPath = basePath.resolve(targetPath);
        }

        // Ensure parent directories exist
        Path parentDir = targetPath.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(targetPath))) {
            oos.writeObject(object);
            logger.debug("Saved object to {}", targetPath);
        } catch (IOException e) {
            logger.error("Failed to save object to {}", targetPath, e);
            throw e;
        }
    }

    /**
     * Loads a serializable object from a file.
     *
     * @param filePath   The file path to load from
     * @param baseDir    The base directory for relative path resolution
     * @param type       The expected class type of the loaded object
     * @param <T>        The type of the object
     * @return The loaded object
     * @throws IOException            If an I/O error occurs
     * @throws ClassNotFoundException If the class of the serialized object cannot be found
     */
    public static <T> T loadObject(String filePath, String baseDir, Class<T> type) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(filePath, "File path cannot be null");
        Objects.requireNonNull(baseDir, "Base directory cannot be null");
        Objects.requireNonNull(type, "Type cannot be null");

        Path basePath = Paths.get(baseDir);
        Path targetPath = Paths.get(filePath);
        if (!targetPath.isAbsolute()) {
            targetPath = basePath.resolve(targetPath);
        }

        if (!Files.exists(targetPath)) {
            logger.warn("File does not exist: {}", targetPath);
            throw new IOException("File does not exist: " + targetPath);
        }

        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(targetPath))) {
            Object obj = ois.readObject();
            if (!type.isInstance(obj)) {
                logger.error("Loaded object from {} is not of expected type {}", targetPath, type.getName());
                throw new ClassCastException("Loaded object is not of type " + type.getName());
            }
            logger.debug("Loaded object from {}", targetPath);
            return type.cast(obj);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to load object from {}", targetPath, e);
            throw e;
        }
    }
}