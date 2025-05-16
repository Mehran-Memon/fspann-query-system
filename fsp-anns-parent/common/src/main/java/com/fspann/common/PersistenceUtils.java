package com.fspann.common;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for serializing and deserializing objects to disk.
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
        }
    }

    /** Reads an object of type T from the given file path. */
    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, TypeReference<T> typeRef)
            throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
            return (T) ois.readObject();
        }
    }

}