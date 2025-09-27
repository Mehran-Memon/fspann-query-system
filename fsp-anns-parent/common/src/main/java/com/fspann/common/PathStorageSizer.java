package com.fspann.common;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class PathStorageSizer implements StorageSizer {
    private final Path root;
    public PathStorageSizer(String rootDir) { this.root = Paths.get(rootDir).toAbsolutePath().normalize(); }
    @Override
    public long bytesOnDisk() {
        if (!Files.exists(root)) return 0L;
        final long[] sum = {0L};
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    sum[0] += Math.max(0L, attrs.size());
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignore) {}
        return sum[0];
    }
}

