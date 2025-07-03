package com.fspann.common;

import java.io.*;

public class PersistenceUtils {
    public static <T extends Serializable> void saveObject(T object, String filePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(object);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T loadObject(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            return (T) ois.readObject();
        }
    }
}