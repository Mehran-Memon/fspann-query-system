package com.fspann.key;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.*;

@DisplayName("SecureKeyDeletion Unit Tests")
public class SecureKeyDeletionTest {

    @Test
    @DisplayName("Test wipeKey handles null key")
    public void testWipeKeyNull() {
        SecureKeyDeletion.wipeKey(null);
    }

    @Test
    @DisplayName("Test wipeKey overwrites key material")
    public void testWipeKeyOverwrites() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        SecretKey key = kg.generateKey();

        byte[] before = key.getEncoded().clone();
        SecureKeyDeletion.wipeKey(key);
        byte[] after = key.getEncoded().clone();

        for (byte b : after) {
            assertEquals(0, b);
        }
    }

    @Test
    @DisplayName("Test wipeBytes handles null")
    public void testWipeBytesNull() {
        SecureKeyDeletion.wipeBytes(null);
    }

    @Test
    @DisplayName("Test wipeBytes handles empty array")
    public void testWipeBytesEmpty() {
        byte[] empty = new byte[0];
        SecureKeyDeletion.wipeBytes(empty);
    }

    @Test
    @DisplayName("Test wipeBytes overwrites data")
    public void testWipeBytesOverwrites() {
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        SecureKeyDeletion.wipeBytes(data);

        for (byte b : data) {
            assertEquals(0, b);
        }
    }
}