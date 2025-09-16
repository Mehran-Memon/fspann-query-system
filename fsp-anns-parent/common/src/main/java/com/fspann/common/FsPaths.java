// common/src/main/java/com/fspann/common/FsPaths.java
package com.fspann.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class FsPaths {
    // ---- property/env keys (used by tests and code) ----
    public static final String BASE_DIR_PROP  = "fspann.baseDir";
    public static final String BASE_DIR_ENV   = "FSPANN_BASE_DIR";
    public static final String METADB_PROP    = "fspann.metadata.dbDir";
    public static final String POINTS_PROP    = "fspann.metadata.pointsDir";
    public static final String KEYSTORE_PROP  = "fspann.keys.storeFile";
    public static final String LOGS_PROP      = "fspann.logs.dir";

    private FsPaths() {}

    public static Path baseDir() {
        String prop = System.getProperty(BASE_DIR_PROP);
        if (prop == null || prop.isBlank()) {
            String env = System.getenv(BASE_DIR_ENV);
            if (env != null && !env.isBlank()) prop = env;
        }
        Path base = (prop == null || prop.isBlank())
                ? Paths.get(System.getProperty("user.dir"))
                : Paths.get(prop);
        return base.toAbsolutePath().normalize();
    }

    public static Path metadataDb() {
        String rel = System.getProperty(METADB_PROP, "metadata/rocksdb");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path pointsDir() {
        String rel = System.getProperty(POINTS_PROP, "metadata/points");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path keyStoreFile() {
        // align with AppBootstrap default
        String rel = System.getProperty(KEYSTORE_PROP, "keys/keystore.blob");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }

    public static Path logsDir() {
        String rel = System.getProperty(LOGS_PROP, "logs");
        Path p = Paths.get(rel);
        return p.isAbsolute() ? p.normalize() : baseDir().resolve(p).normalize();
    }
}
