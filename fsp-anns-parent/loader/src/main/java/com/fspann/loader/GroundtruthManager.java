// loader/src/main/java/com/fspann/loader/GroundtruthManager.java
package com.fspann.loader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** Holds groundtruth neighbors per query. Accepts CSV or IVECS. */
public class GroundtruthManager {
    private final List<int[]> perQuery = new ArrayList<>();

    public void load(String path) throws IOException {
        Path p = Path.of(path);
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".ivecs")) {
            IvecsLoader loader = new IvecsLoader();
            for (Iterator<int[]> it = loader.openIndexIterator(p); it.hasNext(); ) {
                perQuery.add(it.next());
            }
        } else if (name.endsWith(".csv")) {
            try (BufferedReader br = Files.newBufferedReader(p)) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] tok = line.split("[,\\s]+");
                    int[] row = new int[tok.length];
                    for (int i = 0; i < tok.length; i++) row[i] = Integer.parseInt(tok[i]);
                    perQuery.add(row);
                }
            }
        } else {
            throw new IllegalArgumentException("Unsupported groundtruth format: " + name);
        }
    }

    public int[] getGroundtruth(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= perQuery.size())
            throw new IndexOutOfBoundsException("Query index " + queryIndex + " is out of bounds.");
        return perQuery.get(queryIndex);
    }

    public int size() { return perQuery.size(); }
}
