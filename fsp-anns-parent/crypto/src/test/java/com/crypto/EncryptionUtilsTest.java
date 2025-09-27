package com.crypto;

import com.fspann.crypto.EncryptionUtils;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;   // <-- add this import
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.*;

class EncryptionUtilsTest {

    @Test
    void encryptDecryptVector_roundTrip() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        SecretKey k = kg.generateKey();
        byte[] iv = EncryptionUtils.generateIV();

        double[] in = { -1.25, 0.0, 3.75 };
        byte[] ct = EncryptionUtils.encryptVector(in, iv, k);
        double[] out = EncryptionUtils.decryptVector(ct, iv, k);
        assertArrayEquals(in, out, 1e-12);
    }

    @Test
    void invalidIvLengthThrows() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        SecretKey k = kg.generateKey();

        byte[] badIv = new byte[8];
        assertThrows(IllegalArgumentException.class, () ->
                EncryptionUtils.encryptVector(new double[]{1.0}, badIv, k));
    }

    @Test
    void vectorWithNaNThrows() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        SecretKey k = kg.generateKey();
        byte[] iv = EncryptionUtils.generateIV();

        assertThrows(IllegalArgumentException.class, () ->
                EncryptionUtils.encryptVector(new double[]{1.0, Double.NaN}, iv, k));
    }

    @Test
    void decryptFailsWithWrongIvOrKey() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        SecretKey k1 = kg.generateKey();
        SecretKey k2 = kg.generateKey();

        double[] in = { 1.0, 2.0, 3.0 };
        byte[] iv = EncryptionUtils.generateIV();

        byte[] ct = EncryptionUtils.encryptVector(in, iv, k1);

        // Wrong key -> GeneralSecurityException (e.g., AEADBadTagException)
        assertThrows(GeneralSecurityException.class, () -> EncryptionUtils.decryptVector(ct, iv, k2));

        // Wrong IV -> GeneralSecurityException (e.g., AEADBadTagException)
        byte[] otherIv = EncryptionUtils.generateIV();
        assertThrows(GeneralSecurityException.class, () -> EncryptionUtils.decryptVector(ct, otherIv, k1));
    }

    @Test
    void vectorWithInfinityThrows() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        SecretKey k = kg.generateKey();
        byte[] iv = EncryptionUtils.generateIV();

        assertThrows(IllegalArgumentException.class, () ->
                EncryptionUtils.encryptVector(new double[]{1.0, Double.POSITIVE_INFINITY}, iv, k));
    }
}
