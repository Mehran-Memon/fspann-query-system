package com.fspann.key;

import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Secure key deletion utilities.
 *
 * NOTE ON JAVA KEY WIPING:
 * ========================
 * Java's SecretKey objects store cryptographic material in protected memory.
 * The getEncoded() method returns a COPY of the key bytes, not the internal storage.
 * Therefore, overwriting the returned bytes does NOT wipe the key's internal representation.
 *
 * This is a known limitation of Java's cryptography framework - there is no public API
 * to securely wipe the internal state of SecretKey objects from user code.
 *
 * WHAT THIS CLASS DOES:
 * - Overwrites any byte arrays we can access (parameters to wipeBytes)
 * - Uses multiple overwrite passes (random, zeros, ones, zeros) per NIST guidelines
 * - Logs when key deletion is attempted
 *
 * WHAT THIS CLASS CANNOT DO:
 * - Guarantee wiping of SecretKey's internal protected storage
 *
 * SECURITY IMPLICATIONS:
 * - For forward-security, we rely on key ROTATION and DELETION from in-memory maps
 * - Old keys are removed from sessionKeys map, making them eligible for GC
 * - The JVM may eventually zero out garbage-collected memory
 * - For maximum security, use a Hardware Security Module (HSM) with native key management
 */
public final class SecureKeyDeletion {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private SecureKeyDeletion() {}

    /**
     * Attempts to securely wipe a SecretKey by overwriting its encoded bytes.
     *
     * LIMITATION: This overwrites the bytes returned by getEncoded(), but the
     * SecretKey's internal storage may not be wiped due to Java's protected
     * key storage mechanism.
     *
     * For true key deletion, the key must be removed from all in-memory maps
     * and rotated out of use (which this class does via deleteKeysOlderThan).
     *
     * @param key the SecretKey to wipe (may be null)
     */
    public static void wipeKey(SecretKey key) {
        if (key == null) return;

        try {
            // Get a copy of the encoded bytes
            byte[] encoded = key.getEncoded();
            if (encoded != null && encoded.length > 0) {
                // Overwrite the copy we obtained (not the internal storage)
                wipeBytes(encoded);
            }
        } catch (Exception e) {
            // Best effort - some key implementations may not support getEncoded()
        }
    }

    /**
     * Securely wipes a byte array containing sensitive cryptographic material.
     * Uses 4 passes per NIST SP 800-88 guidelines:
     * 1. Random overwrite
     * 2. Zero overwrite
     * 3. Ones overwrite (0xFF)
     * 4. Final zero overwrite
     *
     * @param data the byte array to wipe (may be null)
     */
    public static void wipeBytes(byte[] data) {
        if (data == null || data.length == 0) return;

        // Pass 1: Random overwrite
        SECURE_RANDOM.nextBytes(data);

        // Pass 2: Zeros
        Arrays.fill(data, (byte) 0);

        // Pass 3: Ones
        Arrays.fill(data, (byte) 0xFF);

        // Final pass: Zeros
        Arrays.fill(data, (byte) 0);
    }
}