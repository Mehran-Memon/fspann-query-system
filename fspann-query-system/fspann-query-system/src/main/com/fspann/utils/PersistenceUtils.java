package com.fspann.utils;

import java.io.*;
import java.nio.file.*;

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class    PersistenceUtils {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);

    /**
     * Saves an object to a specified file path.
     *
     * @param obj The object to save.
     * @param path The file path to save the object.
     * @throws IOException If an I/O error occurs.
     */
    public static <T> void saveObject(T obj, String path) throws IOException {
        Path filePath = Paths.get(path);
        // Ensure the parent directory exists
        if (filePath.getParent() != null && !Files.exists(filePath.getParent())) {
            try {
                Files.createDirectories(filePath.getParent()); // Create the directories if they don't exist
            } catch (IOException e) {
                logger.error("Failed to create directories for path: {}", path, e);
                throw e;
            }
        }

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
            oos.writeObject(obj);
            logger.info("Object saved successfully to {}", path);
        } catch (IOException e) {
            logger.error("Error saving object to file: {}", path, e);
            throw e; // Rethrow the exception to ensure it's handled by the caller
        }
    }

    /**
     * Loads an object from a specified file path.
     *
     * @param path The file path from which to load the object.
     * @return The loaded object.
     * @throws IOException If an I/O error occurs.
     * @throws ClassNotFoundException If the class of the object cannot be found.
     */
    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, TypeReference<T> typeRef)
            throws IOException, ClassNotFoundException
    {
        Path filePath = Paths.get(path);
        if (!Files.exists(filePath)) {
            logger.error("File not found: {}", path);
            throw new FileNotFoundException("File not found: " + path);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
            T obj = (T) ois.readObject();
            logger.info("Object loaded successfully from {}", path);
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error loading object from file: {}", path, e);
            throw e; // Rethrow the exception to ensure it's handled by the caller
        }
    }
}
