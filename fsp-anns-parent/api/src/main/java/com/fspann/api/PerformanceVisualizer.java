package com.fspann.api;

import com.fspann.common.QueryResult;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.GrayPaintScale;
import org.jfree.chart.renderer.PaintScale;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.jfree.data.xy.DefaultXYZDataset;

/**
 * Visualizes performance metrics and query results using JFreeChart.
 */
public class PerformanceVisualizer {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceVisualizer.class);

    public static void visualizeTimings(List<Long> timings) {
        if (timings == null || timings.isEmpty()) {
            logger.warn("No timing data to visualize");
            return;
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < timings.size(); i++) {
            dataset.addValue(timings.get(i) / 1_000_000.0, "Time (ms)", "Operation " + (i + 1));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Operation Timings",
                "Operation",
                "Time (ms)",
                dataset
        );

        saveChart(chart, "results_timings.png");
    }


    public static void visualizeConfusionMatrix(int[][] confusionMatrix, int topK) {
        if (confusionMatrix == null || confusionMatrix.length == 0) {
            logger.warn("No confusion matrix data to visualize");
            return;
        }

        DefaultXYZDataset dataset = new DefaultXYZDataset();

        int rows = confusionMatrix.length;
        int cols = confusionMatrix[0].length;

        double[] xValues = new double[rows * cols];
        double[] yValues = new double[rows * cols];
        double[] zValues = new double[rows * cols];

        int index = 0;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                xValues[index] = j;  // X = True Rank
                yValues[index] = i;  // Y = Predicted Rank
                zValues[index] = confusionMatrix[i][j]; // Value
                index++;
            }
        }

        double[][] data = new double[][] { xValues, yValues, zValues };
        dataset.addSeries("Confusion Matrix", data);

        NumberAxis xAxis = new NumberAxis("True Rank");
        NumberAxis yAxis = new NumberAxis("Predicted Rank");

        XYBlockRenderer renderer = new XYBlockRenderer();
        PaintScale scale = new GrayPaintScale(0, Arrays.stream(zValues).max().orElse(1));
        renderer.setPaintScale(scale);
        renderer.setBlockHeight(1.0);
        renderer.setBlockWidth(1.0);

        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);
        plot.setBackgroundPaint(Color.WHITE);

        JFreeChart chart = new JFreeChart("Confusion Matrix", JFreeChart.DEFAULT_TITLE_FONT, plot, false);

        saveChart(chart, "results_confusion_matrix.png");
    }
    public static void visualizeQueryResults(List<QueryResult> results) {
        if (results == null || results.isEmpty()) {
            logger.warn("No query results to visualize");
            return;
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (QueryResult result : results) {
            dataset.addValue(result.getDistance(), "Distance", result.getId());
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Query Results",
                "Neighbor ID",
                "Distance",
                dataset
        );

        saveChart(chart, "results_query_results.png");
    }

    public static void visualizeKNeighbors(List<QueryResult> results, int topK, int dim) {
        if (results == null || results.isEmpty()) {
            logger.warn("No k-neighbors results to visualize");
            return;
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (QueryResult result : results) {
            dataset.addValue(result.getDistance(), "Distance", result.getId());
        }

        JFreeChart chart = ChartFactory.createBarChart(
                String.format("K-Nearest Neighbors (K=%d, D=%d)", topK, dim),
                "Neighbor ID",
                "Distance",
                dataset
        );

        saveChart(chart, "results_kneighbors_d" + dim + ".png");
    }

    public static void visualizeFakePoints(List<double[]> fakePoints, List<double[]> indexedPoints, int dim) {
        logger.info("Visualizing fake points vs indexed points — saved as PNG");

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        for (int i = 0; i < fakePoints.size(); i++) {
            dataset.addValue(fakePoints.get(i)[0], "Fake Point", "F" + i);
        }

        for (int i = 0; i < indexedPoints.size(); i++) {
            dataset.addValue(indexedPoints.get(i)[0], "Indexed Point", "P" + i);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                String.format("Fake Points vs Indexed Points (D=%d)", dim),
                "Point",
                "First Dimension Value",
                dataset
        );

        saveChart(chart, "results_fake_points_d" + dim + ".png");
    }

    public static void visualizeRawData(List<double[]> vectors, int dim, String label) {
        logger.info("Saving raw data visualization: {}", label);

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < vectors.size(); i++) {
            dataset.addValue(vectors.get(i)[0], label, "Vec" + i);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Raw Data " + label,
                "Vector",
                "First Dimension",
                dataset
        );

        saveChart(chart, "results_raw_" + label.replace(" ", "_") + "_d" + dim + ".png");
    }

    public static void visualizeIndexedData(List<double[]> vectors, int dim, String label) {
        logger.info("Saving indexed data visualization: {}", label);

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < vectors.size(); i++) {
            dataset.addValue(vectors.get(i)[0], label, "Vec" + i);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Indexed Data " + label,
                "Vector",
                "First Dimension",
                dataset
        );

        saveChart(chart, "results_indexed_" + label.replace(" ", "_") + "_d" + dim + ".png");
    }

    public static void saveChart(JFreeChart chart, String filename) {
        try {
            ChartUtils.saveChartAsPNG(new File(filename), chart, 1000, 600);
            logger.info("Chart saved to: {}", filename);
        } catch (IOException e) {
            logger.error("Failed to save chart: {}", filename, e);
        }
    }
}
