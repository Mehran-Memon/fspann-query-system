public class ProbeCalculator {
    private static long pow2Round(long x) {
        if (x <= 1) return 1;
        long p = 1;
        while (p < x) p <<= 1;
        // pick nearest power-of-two
        long lower = p >> 1;
        return (p - x) < (x - lower) ? p : lower;
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: java ProbeCalculator <N> <B_or_0> <L> <Fstar>");
            System.out.println("Example: java ProbeCalculator 1000000 8192 8 0.015");
            return;
        }
        long N      = Long.parseLong(args[0]);
        long Binput = Long.parseLong(args[1]);
        int  L      = Integer.parseInt(args[2]);
        double F    = Double.parseDouble(args[3]);

        if (N <= 0 || L <= 0 || F <= 0) {
            throw new IllegalArgumentException("N, L, Fstar must be positive");
        }

        long B = Binput > 0 ? Binput : Math.max(64, Math.min(8192, pow2Round(Math.max(1, N / 120))));
        double Praw = (F * B) / L;
        int P = Math.max(1, (int)Math.round(Praw));

        System.out.println("=== Probe Calculator ===");
        System.out.printf("N=%d  L=%d  F*=%.6f%n", N, L, F);
        System.out.printf("numShards (B): %d%s%n", B, (Binput==0?"  (suggested)":""));
        System.out.printf("probe.perTable (P): %d  [from F*·B/L = %.3f]%n", P, Praw);
        System.out.println("probe.bits.max (r): 1  (try 2 only if recall is low)");
        System.out.println();
        System.out.println("JVM flags:");
        System.out.printf("-Dprobe.perTable=%d -Dprobe.bits.max=1 -Dfanout.target=%s%n",
                P, (F==Math.rint(F) ? String.format("%.0f", F) : Double.toString(F)));
        System.out.println("Note: Set numShards/numTables in config.json (B="+B+", L="+L+").");
    }
}
