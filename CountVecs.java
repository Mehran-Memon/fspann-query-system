import java.io.*;
import java.nio.file.*;

public class CountVecs {
    static void die(String msg) { System.err.println("ERROR: " + msg); System.exit(2); }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) die("Usage: java CountVecs <path> [bvecs|fvecs|auto]");
        Path p = Paths.get(args[0]);
        if (!Files.isReadable(p)) die("File not readable: " + p);
        String mode = (args.length >= 2) ? args[1].toLowerCase() : "auto";

        long fileLen = Files.size(p);
        if (fileLen < 8) die("File too small to be a vecs file");

        int dim;
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
            // BVECs/FVECS store dimension as little-endian int32 at the start of each record.
            dim = Integer.reverseBytes(in.readInt());
        }

        long recB = 4L + dim;        // BVECs: int32 dim + dim bytes
        long recF = 4L + 4L * dim;   // FVECS: int32 dim + dim float32

        boolean okB = (fileLen % recB) == 0;
        boolean okF = (fileLen % recF) == 0;

        if ("auto".equals(mode)) {
            // prefer extension hint
            String ext = p.getFileName().toString().toLowerCase();
            boolean hintB = ext.endsWith(".bvecs") || ext.endsWith(".siftbin");
            boolean hintF = ext.endsWith(".fvecs");
            if (okB && !okF) mode = "bvecs";
            else if (okF && !okB) mode = "fvecs";
            else if (hintB) mode = "bvecs";
            else if (hintF) mode = "fvecs";
            else mode = okB ? "bvecs" : "fvecs"; // last resort
        }

        long rec = "bvecs".equals(mode) ? recB : recF;
        if (fileLen % rec != 0) die("Record size mismatch for " + mode + " (dim=" + dim + "). FileLen=" + fileLen + ", rec=" + rec);

        long count = fileLen / rec;

        System.out.println("File    : " + p.toAbsolutePath());
        System.out.println("Format  : " + mode);
        System.out.println("Dim     : " + dim);
        System.out.println("Vectors : " + count);
    }
}
