package com.loader;

import com.fspann.loader.BvecsLoader;
import com.fspann.loader.FormatLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class BvecsLoaderTest {

    @TempDir
    Path tmp;

    private static Path writeBvecs(Path p, int dim, int[][] rows) throws IOException {
        try (FileChannel ch = FileChannel.open(p, CREATE, TRUNCATE_EXISTING, WRITE)) {
            for (int[] r : rows) {
                ByteBuffer bb = ByteBuffer.allocate(4 + dim).order(ByteOrder.LITTLE_ENDIAN);
                bb.putInt(dim);
                for (int v : r) bb.put((byte) (v & 0xFF));
                bb.flip();
                ch.write(bb);
            }
        }
        return p;
    }

    @Test
    void readsUnsignedBytesAsDoubles() throws Exception {
        int dim = 3;
        Path f = writeBvecs(tmp.resolve("x.bvecs"), dim, new int[][]{
                {0, 127, 255},
                {10, 20, 30}
        });

        FormatLoader l = new BvecsLoader();
        Iterator<double[]> it = l.openVectorIterator(f);

        List<double[]> all = new ArrayList<>();
        while (it.hasNext()) all.add(it.next());

        assertEquals(2, all.size());
        assertArrayEquals(new double[]{0.0, 127.0, 255.0}, all.get(0), 1e-9);
        assertArrayEquals(new double[]{10.0, 20.0, 30.0},  all.get(1), 1e-9);
    }
}
