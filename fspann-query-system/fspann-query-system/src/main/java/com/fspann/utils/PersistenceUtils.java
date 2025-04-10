package java.com.fspann.utils;

import java.io.*;

public class PersistenceUtils {

    public static <T> void saveObject(T obj, String path) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
            oos.writeObject(obj);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T loadObject(String path, Class<T> type) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
            return (T) ois.readObject();
        }
    }
}
