package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;

public class PersistenceUtils {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceUtils.class);
    private static final Set<String> ALLOWED_CLASSES = Set.of(
            "com.fspann.common.EncryptedPoint",
            "com.fspann.key.KeyManager$KeyStoreBlob",
            "com.fspann.common.KeyVersion",
            "java.util.ArrayList",
            "java.util.HashMap",
            "java.util.concurrent.ConcurrentHashMap",
            "javax.crypto.spec.SecretKeySpec"
    );

    /**
     * Saves a serializable object to the specified file path with buffering.
     * Validates that the file path is within the expected directory.
     */
    public static <T extends Serializable> void saveObject(T object, String filePath, String baseDir) throws IOException {
        Objects.requireNonNull(object, "Object cannot be null");
        Objects.requireNonNull(filePath, "File path cannot be null");
        Objects.requireNonNull(baseDir, "Base directory cannot be null");

        Path path = Paths.get(filePath).normalize();
        Path basePath = Paths.get(baseDir).normalize();
        if (!path.startsWith(basePath)) {
            logger.error("Path traversal detected: {}", filePath);
            throw new IOException("Invalid file path: " + filePath);
        }

        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(path));
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            logger.debug("Saved object to {}", filePath);
        } catch (IOException e) {
            logger.error("Failed to save object to {}", filePath, e);
            throw new IOException("Failed to save object to " + filePath, e);
        }
    }

    /**
     * Loads a serializable object from the specified file path with type safety.
     * Restricts deserialization to allowed classes and validates file path.
     */
    public static <T extends Serializable> T loadObject(String filePath, String baseDir, Class<T> expectedType)
            throws IOException, ClassNotFoundException {
        Objects.requireNonNull(filePath, "File path cannot be null");
        Objects.requireNonNull(baseDir, "Base directory cannot be null");
        Objects.requireNonNull(expectedType, "Expected type cannot be null");

        Path path = Paths.get(filePath).normalize();
        Path basePath = Paths.get(baseDir).normalize();
        if (!path.startsWith(basePath)) {
            logger.error("Path traversal detected: {}", filePath);
            throw new IOException("Invalid file path: " + filePath);
        }

        if (!Files.exists(path)) {
            logger.warn("File does not exist: {}", filePath);
            throw new IOException("File does not exist: " + filePath);
        }

        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(path));
             ValidatingObjectInputStream ois = new ValidatingObjectInputStream(bis)) {
            ois.accept(ALLOWED_CLASSES);
            Object obj = ois.readObject();
            if (!expectedType.isInstance(obj)) {
                logger.error("Deserialized object is not of expected type: {}", expectedType.getName());
                throw new ClassCastException("Expected " + expectedType.getName() + ", got " + obj.getClass().getName());
            }
            logger.debug("Loaded object from {}", filePath);
            return expectedType.cast(obj);
        } catch (IOException e) {
            logger.error("Failed to load object from {}", filePath, e);
            throw new IOException("Failed to load object from " + filePath, e);
        } catch (ClassNotFoundException e) {
            logger.error("Class not found while loading object from {}", filePath, e);
            throw e;
        }
    }

    /**
     * Custom ObjectInputStream to restrict deserialization to allowed classes.
     */
    private static class ValidatingObjectInputStream extends ObjectInputStream {
        private final Set<String> allowedClasses;

        public ValidatingObjectInputStream(InputStream in) throws IOException {
            super(in);
            this.allowedClasses = ALLOWED_CLASSES;
        }

        public void accept(Set<String> allowedClasses) {
            this.allowedClasses.addAll(allowedClasses);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String className = desc.getName();
            if (!allowedClasses.contains(className)) {
                logger.error("Attempted to deserialize unauthorized class: {}", className);
                throw new InvalidClassException("Unauthorized class: " + className);
            }
            return super.resolveClass(desc);
        }
    }
}