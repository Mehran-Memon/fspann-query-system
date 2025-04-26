package com.fspann.utils;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceUtils {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);

    /**
     * Saves an object to a specified file path.
     *
     * @param obj The object to save.
     * @param path The file path to save the object.
     * @throws IOException If an I/O error occurs.
     */
    public static <T> void saveObject(T obj, String path) throws IOException {
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
     * @param type The class type of the object to load.
     * @return The loaded object.
     * @throws IOException If an I/O error occurs.
     * @throws ClassNotFoundException If the class of the object cannot be found.
     */
    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, Class<T> type) throws IOException, ClassNotFoundException {
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
