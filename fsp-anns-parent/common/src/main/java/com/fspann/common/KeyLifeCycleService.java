package com.fspann.common;

/**
 * KeyLifeCycleService manages cryptographic key versions,
 * rotation policies, and forward security enforcement.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Maintains current and past keys</li>
 *   <li>Rotates keys after defined usage thresholds</li>
 *   <li>Enables re-encryption of stored data to maintain forward security</li>
 * </ul>
 */
public interface KeyLifeCycleService {

    /**
     * Rotates to a new key version if policy conditions are met.
     * This may trigger a re-key and version increment.
     */
    void rotateIfNeeded();

    /**
     * Notifies the lifecycle manager that a key-dependent operation was performed.
     * Used for threshold-based rotation policies (e.g., after N encryptions).
     */
    void incrementOperation();

    /**
     * Gets the current active key version.
     *
     * @return the current KeyVersion
     */
    KeyVersion getCurrentVersion();

    /**
     * Gets the immediately previous key version (if still available).
     * This is typically used for transitional decryption.
     *
     * @return the previous KeyVersion
     */
    KeyVersion getPreviousVersion();

    /**
     * Returns a specific KeyVersion by version number.
     * If the key is expired or removed, it throws an exception.
     *
     * @param version the key version number
     * @return the corresponding KeyVersion
     * @throws IllegalArgumentException if the version is unknown
     */
    KeyVersion getVersion(int version);

    /**
     * Triggers re-encryption of all encrypted vectors using the latest key.
     * This is part of the forward-security pipeline and must ensure versioned metadata is updated.
     */
    void reEncryptAll();
}
