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

    /**
     * REVISED: This test documents a Java limitation.
     *
     * Java's SecretKey.getEncoded() returns a COPY of the key material,
     * not the internal storage. Therefore, overwriting the copy does NOT
     * wipe the SecretKey's internal representation.
     *
     * This is a known JDK limitation - there's no public API to wipe
     * the internal storage of a SecretKey object.
     *
     * Our SecureKeyDeletion.wipeKey() does its best effort by wiping
     * whatever bytes it can access, but cannot guarantee wiping the
     * SecretKey's internal protected memory.
     *
     * For true cryptographic key deletion, rely on:
     * 1. Removing keys from in-memory maps (enables GC)
     * 2. Key rotation (moving to fresh keys)
     * 3. Hardware Security Modules (HSM) for maximum security
     */
    @Test
    @DisplayName("Test wipeKey demonstrates Java SecretKey limitation")
    public void testWipeKeyOverwrites() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        SecretKey key = kg.generateKey();

        byte[] before = key.getEncoded().clone();

        // This wipes the bytes we received (a copy)
        SecureKeyDeletion.wipeKey(key);

        // But the SecretKey's internal storage is unchanged
        // because getEncoded() returns a NEW COPY each time
        byte[] after = key.getEncoded().clone();

        // The bytes we got are NOT all zeros (Java limitation)
        // The SecretKey still holds the original material internally
        assertFalse(Arrays.equals(before, new byte[before.length]),
                "Sanity check: key material should not be all zeros initially");

        // This demonstrates that wipeKey() cannot fully wipe a SecretKey
        // It can only wipe bytes it has access to (which is a copy)
        byte[] zeroArray = new byte[after.length];
        assertFalse(Arrays.equals(after, zeroArray),
                "Java limitation: SecretKey internal storage cannot be wiped via public API");
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

    /**
     * This test shows that wipeBytes() DOES work correctly
     * on raw byte arrays that we control.
     */
    @Test
    @DisplayName("Test wipeBytes overwrites data")
    public void testWipeBytesOverwrites() {
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        SecureKeyDeletion.wipeBytes(data);

        // This DOES work - we can wipe byte arrays we control
        for (byte b : data) {
            assertEquals(0, b);
        }
    }

    /**
     * Test that shows the difference: wipeBytes on a direct byte array works,
     * but wipeKey cannot guarantee wiping SecretKey's internal storage.
     */
    @Test
    @DisplayName("Test wipeBytes vs wipeKey - demonstrates the limitation")
    public void testWipeBytesVsWipeKey() {
        // Direct byte array: we CAN wipe it
        byte[] directBytes = new byte[32];
        Arrays.fill(directBytes, (byte) 0x42);
        SecureKeyDeletion.wipeBytes(directBytes);
        for (byte b : directBytes) {
            assertEquals(0, b, "Direct byte array is properly wiped");
        }

        // SecretKey: we CANNOT fully wipe its internal storage
        try {
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            kg.init(256, SecureRandom.getInstanceStrong());
            SecretKey key = kg.generateKey();

            SecureKeyDeletion.wipeKey(key);

            // The SecretKey's internal material is still there (Java limitation)
            byte[] material = key.getEncoded();
            assertNotNull(material, "SecretKey still returns key material");
            assertTrue(material.length > 0, "SecretKey material is not empty");

            // This is expected and documented - not a bug in our wipeKey()
            // but a limitation of Java's cryptography framework
        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        }
    }
}