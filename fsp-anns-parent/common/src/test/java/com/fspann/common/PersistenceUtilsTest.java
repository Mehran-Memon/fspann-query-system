
// File: src/test/java/com/fspann/common/PersistenceUtilsTest.java
package com.fspann.common;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceUtilsTest {
    @TempDir Path tempDir;

    @Test
    void saveAndLoadSerializableObject() throws IOException, ClassNotFoundException {
        Path file = tempDir.resolve("map.ser");
        Map<String,String> original = new HashMap<>();
        original.put("k1","v1");
        original.put("k2","v2");

        PersistenceUtils.saveObject((java.io.Serializable)original, file.toString());
        TypeReference<Map<String,String>> typeRef = new TypeReference<>() {};
        Map<String,String> loaded = PersistenceUtils.loadObject(file.toString(), typeRef);
        assertEquals(original, loaded);
    }

    @Test
    void loadMissingFileThrows() {
        Path file = tempDir.resolve("nofile.ser");
        TypeReference<Map<String,String>> typeRef = new TypeReference<>() {};
        assertThrows(IOException.class, () -> PersistenceUtils.loadObject(file.toString(), typeRef));
    }

}

