// Save as BvecsHead.java, compile: javac BvecsHead.java, run: java BvecsHead <in> <out> <N>
import java.io.*;
import java.nio.file.*;

public class BvecsHead {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) { System.err.println("usage: in.bvecs out.bvecs N"); System.exit(1); }
        Path in = Path.of(args[0]); Path out = Path.of(args[1]); long N = Long.parseLong(args[2]);
        try (DataInputStream din = new DataInputStream(new BufferedInputStream(Files.newInputStream(in)));
             DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(out)))) {
            for (long i=0; i<N; i++) {
                int dim = Integer.reverseBytes(din.readInt());
                dout.writeInt(Integer.reverseBytes(dim));
                byte[] vec = din.readNBytes(dim);
                if (vec.length != dim) break; // EOF
                dout.write(vec);
            }
        }
    }
}
