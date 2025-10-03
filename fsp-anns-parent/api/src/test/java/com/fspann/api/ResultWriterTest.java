package com.fspann.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResultWriterTest {

    @TempDir
    Path tmp;

    @Test
    void writesHeaderOnce_andPrefixesWithSection() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw = new ResultWriter(out);

        String title1 = "Query 7 Results (dim=128)";
        String[] cols = {"TopK","Retrieved","LiteratureRatio","Precision","TimeMs"};
        List<String[]> rows1 = Collections.singletonList(
                new String[]{"10","10","1.1111","0.5000","12.3"}
        );
        rw.writeTable(title1, cols, rows1);

        String title2 = "Query 8 Results (dim=128)";
        List<String[]> rows2 = Collections.singletonList(
                new String[]{"20","20","1.2222","0.8000","15.7"}
        );
        rw.writeTable(title2, cols, rows2);

        String csv = Files.readString(out);
        String[] lines = csv.split("\\R");

        assertTrue(lines[0].startsWith("Section,TopK,"), "Header must be first and appear once");
        assertEquals(3, lines.length, "Header + 2 data lines expected");

        assertTrue(lines[1].startsWith(title1 + ","), "First data row should start with Section=title1");
        assertTrue(lines[2].startsWith(title2 + ","), "Second data row should start with Section=title2");
    }

    @Test
    void headerNotDuplicated_whenAppendingWithNewInstance() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw1 = new ResultWriter(out);
        String[] cols = {"TopK","Retrieved","LiteratureRatio","Precision","TimeMs"};
        rw1.writeTable("Query 1 Results (dim=32)", cols,
                Collections.singletonList(new String[]{"1","1","1.0","1.0","0.1"}));

        // New instance, same file → header should not repeat
        ResultWriter rw2 = new ResultWriter(out);
        rw2.writeTable("Query 2 Results (dim=32)", cols,
                Collections.singletonList(new String[]{"1","1","1.0","1.0","0.1"}));

        String[] lines = Files.readString(out).split("\\R");
        assertTrue(lines[0].startsWith("Section,TopK,")); // one header
        assertEquals(3, lines.length);                    // header + 2 rows
    }

    @Test
    void escapesSectionTitle_whenItHasCommaOrQuote() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw = new ResultWriter(out);
        String[] cols = {"TopK","Retrieved"};
        String title = "Query 3, Results \"special\"";

        rw.writeTable(title, cols,
                Collections.singletonList(new String[]{"5","5"}));

        String line = Files.readAllLines(out).get(1);
        assertTrue(line.startsWith("\"Query 3, Results \"\"special\"\"\",5,5"), line);
    }
}
