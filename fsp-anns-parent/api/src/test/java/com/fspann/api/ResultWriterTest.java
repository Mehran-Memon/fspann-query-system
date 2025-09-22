package com.fspann.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResultWriterTest {

    @TempDir
    Path tmp;

    @Test
    void writesHeaderOnce_andPrefixesWithQIndex() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw = new ResultWriter(out);

        String title1 = "Query 7 Results (dim=128)";
        String[] cols = {"TopK","Retrieved","LiteratureRatio","Precision","TimeMs"};
        java.util.List<String[]> rows1 = new java.util.ArrayList<>();
        rows1.add(new String[]{"10","10","1.1111","0.5000","12.3"});
        rw.writeTable(title1, cols, rows1);

        String title2 = "Query 8 Results (dim=128)";
        java.util.List<String[]> rows2 = new java.util.ArrayList<>();
        rows2.add(new String[]{"20","20","1.2222","0.8000","15.7"});
        rw.writeTable(title2, cols, rows2);

        String csv = Files.readString(out);
        String[] lines = csv.split("\\R");

        assertTrue(lines[0].startsWith("qIndex,Section,TopK,"), "Header must be first and appear once");
        assertEquals(3, lines.length, "Header + 2 data lines expected");

        // Section titles aren’t quoted unless needed, so don’t expect quotes here
        assertTrue(lines[1].startsWith("7," + title1), "First data row should carry qIndex=7");
        assertTrue(lines[2].startsWith("8," + title2), "Second data row should carry qIndex=8");
    }


    @Test
    void headerNotDuplicated_whenAppendingWithNewInstance() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw1 = new ResultWriter(out);
        String[] cols = {"TopK","Retrieved","LiteratureRatio","Precision","TimeMs"};
        rw1.writeTable("Query 1 Results (dim=32)", cols,
                java.util.Arrays.asList(new String[][]{ new String[]{"1","1","1.0","1.0","0.1"} }));

        // New instance, same file
        ResultWriter rw2 = new ResultWriter(out);
        rw2.writeTable("Query 2 Results (dim=32)", cols,
                java.util.Arrays.asList(new String[][]{ new String[]{"1","1","1.0","1.0","0.1"} }));

        String[] lines = Files.readString(out).split("\\R");
        assertTrue(lines[0].startsWith("qIndex,Section,TopK,")); // one header
        assertEquals(3, lines.length);                           // header + 2 rows
    }

    @Test
    void escapesSectionTitle_whenItHasCommaOrQuote() throws Exception {
        Path out = tmp.resolve("results.csv");
        ResultWriter rw = new ResultWriter(out);
        String[] cols = {"TopK","Retrieved"};
        String title = "Query 3, Results \"special\"";
        rw.writeTable(title, cols,
                java.util.Arrays.asList(new String[][]{ new String[]{"5","5"} }));
        String line = Files.readAllLines(out).get(1);
        assertTrue(line.startsWith("3,\"Query 3, Results \"\"special\"\"\""), line);
    }

}
