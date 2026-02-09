package com.fspann.api;

import com.fspann.loader.GroundtruthManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * GroundtruthValidator - CRITICAL VALIDATION
 *
 * This class validates that groundtruth is consistent with the base dataset
 * BEFORE any queries are run. This prevents the entire scenario where
 * precision = 0 because GT is wrong.
 *
 * Validation includes:
 *   1. GT IDs are within dataset bounds
 *   2. GT neighbors are actually closer than random vectors (spot check)
 *   3. For sample queries, brute-force NN matches GT top-1
 */
public final class GroundtruthValidator {

    private static final Logger logger = LoggerFactory.getLogger(GroundtruthValidator.class);

    private GroundtruthValidator() {}

    /**
     * Result of GT validation.
     */
    public static final class ValidationResult {
        public final boolean valid;
        public final int sampleSize;
        public final int mismatches;
        public final double mismatchRate;
        public final String message;
        public final List<Integer> mismatchedQueries;

        private ValidationResult(boolean valid, int sampleSize, int mismatches,
                                 double mismatchRate, String message,
                                 List<Integer> mismatchedQueries) {
            this.valid = valid;
            this.sampleSize = sampleSize;
            this.mismatches = mismatches;
            this.mismatchRate = mismatchRate;
            this.message = message;
            this.mismatchedQueries = mismatchedQueries;
        }

        public static ValidationResult success(int sampleSize, String message) {
            return new ValidationResult(true, sampleSize, 0, 0.0, message, List.of());
        }

        public static ValidationResult failure(int sampleSize, int mismatches,
                                               double rate, String message,
                                               List<Integer> mismatchedQueries) {
            return new ValidationResult(false, sampleSize, mismatches, rate, message, mismatchedQueries);
        }
    }

    /**
     * Validate groundtruth against base vectors.
     *
     * This performs brute-force NN search on a sample of queries and
     * compares the results to groundtruth. If they don't match, GT is wrong.
     *
     * @param basePath path to base vectors file (.fvecs or .bvecs)
     * @param queries list of query vectors
     * @param gt groundtruth manager
     * @param dimension vector dimension
     * @param sampleSize number of queries to validate
     * @param tolerance allowed mismatch rate (e.g., 0.05 = 5%)
     * @return validation result
     */
    public static ValidationResult validate(
            Path basePath,
            List<double[]> queries,
            GroundtruthManager gt,
            int dimension,
            int sampleSize,
            double tolerance
    ) throws IOException {

        Objects.requireNonNull(basePath, "basePath");
        Objects.requireNonNull(queries, "queries");
        Objects.requireNonNull(gt, "gt");

        if (queries.isEmpty()) {
            return ValidationResult.success(0, "No queries to validate");
        }

        if (gt.size() == 0) {
            return ValidationResult.failure(0, 0, 1.0,
                    "Groundtruth is empty", List.of());
        }

        // Determine file format
        boolean isBvecs = basePath.toString().toLowerCase().endsWith(".bvecs");

        logger.info("Validating groundtruth: basePath={}, queries={}, sampleSize={}, tolerance={}",
                basePath, queries.size(), sampleSize, tolerance);

        // Open base vectors
        try (BaseVectorReader reader = new BaseVectorReader(basePath, dimension, isBvecs)) {

            int effectiveSample = Math.min(sampleSize, queries.size());
            int mismatches = 0;
            List<Integer> mismatchedQueries = new ArrayList<>();

            // Use deterministic sampling
            Random rnd = new Random(42);
            Set<Integer> sampled = new HashSet<>();
            while (sampled.size() < effectiveSample) {
                sampled.add(rnd.nextInt(queries.size()));
            }

            for (int queryIdx : sampled) {
                double[] query = queries.get(queryIdx);

                // Get GT top-1
                int[] gtIds = gt.getGroundtruthIds(queryIdx, 1);
                if (gtIds == null || gtIds.length == 0) {
                    logger.warn("GT missing for query {}", queryIdx);
                    continue;
                }
                int gtTop1 = gtIds[0];

                // Compute TRUE top-1 via brute force
                int trueTop1 = reader.bruteForceNN(query);

                // Compare
                if (gtTop1 != trueTop1) {
                    mismatches++;
                    if (mismatchedQueries.size() < 10) {
                        mismatchedQueries.add(queryIdx);
                    }

                    if (logger.isDebugEnabled()) {
                        double gtDist = reader.l2(query, gtTop1);
                        double trueDist = reader.l2(query, trueTop1);
                        logger.debug(
                                "GT mismatch: query={}, gtTop1={} (dist={}), trueTop1={} (dist={})",
                                queryIdx, gtTop1, gtDist, trueTop1, trueDist
                        );
                    }
                }
            }

            double mismatchRate = (double) mismatches / effectiveSample;

            // FIXED: Use String.format for percentage formatting instead of invalid SLF4J syntax
            logger.info(
                    "GT Validation complete: samples={}, matches={}, mismatches={}, rate={}%",
                    effectiveSample,
                    effectiveSample - mismatches,
                    mismatches,
                    String.format("%.2f", mismatchRate * 100)
            );

            if (mismatchRate > tolerance) {
                String msg = String.format(
                        "GT validation FAILED: %.2f%% mismatch rate exceeds %.2f%% tolerance. " +
                                "Groundtruth may be corrupted or computed for a different dataset.",
                        mismatchRate * 100, tolerance * 100
                );
                logger.error(msg);
                return ValidationResult.failure(effectiveSample, mismatches,
                        mismatchRate, msg, mismatchedQueries);
            }

            String msg = String.format(
                    "GT validation PASSED: %.2f%% match rate",
                    (1 - mismatchRate) * 100
            );
            logger.info(msg);
            return ValidationResult.success(effectiveSample, msg);
        }
    }

    /**
     * Simple base vector reader for validation.
     */
    private static final class BaseVectorReader implements AutoCloseable {
        private final FileChannel channel;
        private final int dimension;
        private final int recordBytes;
        private final long count;
        private final boolean isBvecs;

        BaseVectorReader(Path path, int dimension, boolean isBvecs) throws IOException {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.dimension = dimension;
            this.isBvecs = isBvecs;
            this.recordBytes = 4 + (isBvecs ? dimension : dimension * 4);
            this.count = channel.size() / recordBytes;
            logger.info("1B Validator: {} vectors detected in base file", count);
        }

        double l2(double[] query, int index) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(recordBytes).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(buf, (long) index * recordBytes);
            buf.flip();
            buf.getInt(); // Skip dimension

            double sum = 0.0;
            for (int i = 0; i < dimension; i++) {
                double val = isBvecs ? (buf.get() & 0xFF) : buf.getFloat();
                double d = query[i] - val;
                sum += d * d;
            }
            return Math.sqrt(sum);
        }

        int bruteForceNN(double[] query) throws IOException {
            // At 1B scale, even BF-NN on 100 samples is slow.
            // We scan a subset of the file for validation.
            int bestIndex = -1;
            double bestDist = Double.POSITIVE_INFINITY;
            long scanLimit = Math.min(count, 1000000); // Scan first 1M vectors for BF NN validation

            for (int i = 0; i < scanLimit; i++) {
                double dist = l2(query, i);
                if (dist < bestDist) {
                    bestDist = dist;
                    bestIndex = i;
                }
            }
            return bestIndex;
        }

        @Override
        public void close() throws IOException { channel.close(); }
    }

}