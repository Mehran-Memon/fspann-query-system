package com.loader;

import com.fspann.loader.CsvLoader;
import com.fspann.loader.FormatLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvLoaderTest {

    @TempDir
    Path tmp;

    @Test
    void vectorCsv_withHeaderAndComments_isParsed() throws Exception {
        Path f = tmp.resolve("vec.csv");
        String csv = """
            # comment
            id, x, y , z
            1.0, 2.0 , 3.0
            4.0  5.0   6.0
            7.0,8.0,9.0,
            """;
        Files.writeString(f, csv);

        FormatLoader l = new CsvLoader();
        Iterator<double[]> it = l.openVectorIterator(f);

        List<double[]> all = new ArrayList<>();
        while (it.hasNext()) all.add(it.next());

        assertEquals(3, all.size());
        assertArrayEquals(new double[]{1.0,2.0,3.0}, all.get(0), 1e-9);
        assertArrayEquals(new double[]{4.0,5.0,6.0}, all.get(1), 1e-9);
        assertArrayEquals(new double[]{7.0,8.0,9.0}, all.get(2), 1e-9);
    }

    @Test
    void indexCsv_isParsedAsIntArrays() throws Exception {
        Path f = tmp.resolve("gt.csv");
        String csv = """
            # k-NN indices, one row per query
            0, 1, 2
            10 11 12
            5,5,7,
            """;
        Files.writeString(f, csv);

        FormatLoader l = new CsvLoader();
        Iterator<int[]> it = l.openIndexIterator(f);

        List<int[]> rows = new ArrayList<>();
        while (it.hasNext()) rows.add(it.next());

        assertEquals(3, rows.size());
        assertArrayEquals(new int[]{0,1,2}, rows.get(0));
        assertArrayEquals(new int[]{10,11,12}, rows.get(1));
        assertArrayEquals(new int[]{5,5,7}, rows.get(2));
    }
}
