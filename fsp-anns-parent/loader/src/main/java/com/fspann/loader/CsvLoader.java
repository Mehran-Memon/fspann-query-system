package com.fspann.loader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Streaming CSV loader for vectors or integer indices.
 * - Skips blank lines and lines starting with '#' (comments).
 * - Tolerates a single header line: if the FIRST non-empty line is non-numeric, it is skipped once.
 * - Tolerates extra spaces and trailing commas.
 * - Handles UTF-8 BOM if present.
 */
public class CsvLoader implements FormatLoader {

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        BufferedReader br = newBufferedReader(file);
        return new Iterator<>() {
            String nextLine;
            boolean headerChecked = false;

            {
                nextLine = fetchNextDataLine(/*numeric=*/true);
            }

            private String fetchNextDataLine(boolean numeric) {
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = sanitize(line);
                        if (line.isEmpty() || line.charAt(0) == '#') continue;

                        if (!headerChecked) {
                            headerChecked = true;
                            // First candidate line: if not numeric, treat as header and continue once
                            if (numeric ? !isAllNumeric(line) : !isAllIntegers(line)) {
                                continue;
                            }
                        } else {
                            // After the first data line, just skip malformed lines silently
                            if (numeric ? !isAllNumeric(line) : !isAllIntegers(line)) {
                                continue;
                            }
                        }
                        return line;
                    }
                    return null;
                } catch (IOException e) {
                    closeQuietly(br);
                    throw new UncheckedIOException(e);
                }
            }

            @Override public boolean hasNext() { return nextLine != null; }

            @Override public double[] next() {
                if (nextLine == null) throw new NoSuchElementException();
                try {
                    double[] v = parseDoubles(nextLine);
                    nextLine = fetchNextDataLine(/*numeric=*/true);
                    return v;
                } finally {
                    if (nextLine == null) closeQuietly(br);
                }
            }
        };
    }

    @Override
    public Iterator<int[]> openIndexIterator(Path file) throws IOException {
        BufferedReader br = newBufferedReader(file);
        return new Iterator<>() {
            String nextLine;
            boolean headerChecked = false;

            {
                nextLine = fetchNextDataLine(/*numeric=*/false);
            }

            private String fetchNextDataLine(boolean numeric) {
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = sanitize(line);
                        if (line.isEmpty() || line.charAt(0) == '#') continue;

                        if (!headerChecked) {
                            headerChecked = true;
                            if (numeric ? !isAllNumeric(line) : !isAllIntegers(line)) {
                                continue; // skip header once
                            }
                        } else {
                            if (numeric ? !isAllNumeric(line) : !isAllIntegers(line)) {
                                continue;
                            }
                        }
                        return line;
                    }
                    return null;
                } catch (IOException e) {
                    closeQuietly(br);
                    throw new UncheckedIOException(e);
                }
            }

            @Override public boolean hasNext() { return nextLine != null; }

            @Override public int[] next() {
                if (nextLine == null) throw new NoSuchElementException();
                try {
                    int[] v = parseInts(nextLine);
                    nextLine = fetchNextDataLine(/*numeric=*/false);
                    return v;
                } finally {
                    if (nextLine == null) closeQuietly(br);
                }
            }
        };
    }

    /* -------------------- helpers -------------------- */

    private static BufferedReader newBufferedReader(Path file) throws IOException {
        InputStream in = Files.newInputStream(file);
        // Strip UTF-8 BOM if present
        PushbackInputStream pb = new PushbackInputStream(in, 3);
        byte[] bom = new byte[3];
        int n = pb.read(bom, 0, 3);
        if (n == 3 && !(bom[0] == (byte)0xEF && bom[1] == (byte)0xBB && bom[2] == (byte)0xBF)) {
            pb.unread(bom, 0, 3);
        } else if (n > 0 && n < 3) {
            pb.unread(bom, 0, n);
        }
        return new BufferedReader(new InputStreamReader(pb, StandardCharsets.UTF_8));
    }

    private static String sanitize(String s) {
        s = s.trim();
        while (!s.isEmpty() && (s.endsWith(",") || Character.isWhitespace(s.charAt(s.length() - 1)))) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s;
    }

    private static boolean isAllNumeric(String line) {
        String[] toks = splitFlexible(line);
        if (toks.length == 0) return false;
        for (String t : toks) if (!isDouble(t)) return false;
        return true;
    }

    private static boolean isAllIntegers(String line) {
        String[] toks = splitFlexible(line);
        if (toks.length == 0) return false;
        for (String t : toks) if (!isInt(t)) return false;
        return true;
    }

    private static double[] parseDoubles(String line) {
        String[] toks = splitFlexible(line);
        double[] v = new double[toks.length];
        for (int i = 0; i < toks.length; i++) v[i] = Double.parseDouble(toks[i]);
        return v;
    }

    private static int[] parseInts(String line) {
        String[] toks = splitFlexible(line);
        int[] v = new int[toks.length];
        for (int i = 0; i < toks.length; i++) v[i] = Integer.parseInt(toks[i]);
        return v;
    }

    private static String[] splitFlexible(String line) {
        return Arrays.stream(line.split("[,\\s]+"))
                .filter(tok -> !tok.isEmpty())
                .toArray(String[]::new);
    }

    private static boolean isDouble(String s) {
        try { Double.parseDouble(s); return true; } catch (Exception e) { return false; }
    }

    private static boolean isInt(String s) {
        try { Integer.parseInt(s); return true; } catch (Exception e) { return false; }
    }

    private static void closeQuietly(Closeable c) {
        try { c.close(); } catch (IOException ignored) {}
    }
}
