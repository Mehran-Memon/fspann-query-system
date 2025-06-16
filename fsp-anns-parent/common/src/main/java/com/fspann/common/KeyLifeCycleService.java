package com.fspann.common;

public interface KeyLifeCycleService {
    void rotateIfNeeded();
    void incrementOperation();
    KeyVersion getCurrentVersion();
    KeyVersion getPreviousVersion();
    KeyVersion getVersion(int version); // Updated to match usage
}