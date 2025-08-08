// com/fspann/common/FsPaths.java
package com.fspann.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class FsPaths {
    private FsPaths() {}

    public static Path baseDir() {
        String prop = System.getProperty("fspann.baseDir");
        if (prop == null || prop.isBlank()) {
            String env = System.getenv("FSPANN_BASE_DIR");
            if (env != null && !env.isBlank()) prop = env;
        }
        return (prop == null || prop.isBlank())
                ? Paths.get("").toAbsolutePath().normalize()
                : Paths.get(prop).toAbsolutePath().normalize();
    }

    public static Path metadataDb() {
        String rel = System.getProperty("fspann.metadata.dbDir", "metadata/rocksdb");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path pointsDir() {
        String rel = System.getProperty("fspann.metadata.pointsDir", "metadata/points");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path keyStoreFile() {
        String rel = System.getProperty("fspann.keys.storeFile", "secrets/keystore.blob");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path logsDir() {
        String rel = System.getProperty("fspann.logs.dir", "logs");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }
}
