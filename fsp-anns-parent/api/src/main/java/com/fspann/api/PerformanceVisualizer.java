package com.fspann.api;

import com.fspann.common.QueryResult;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.Map;

/**
 * Enhanced visualizer for performance metrics, data, and encryption keys using JFreeChart.
 */
public class PerformanceVisualizer {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceVisualizer.class);

    /**
     * Visualizes a list of timing measurements.
     */
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
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Timings", chartPanel);
    }

    /**
     * Visualizes a confusion matrix for query accuracy.
     */
    public static void visualizeConfusionMatrix(int[][] confusionMatrix, int topK) {
        if (confusionMatrix == null || confusionMatrix.length == 0) {
            logger.warn("No confusion matrix data to visualize");
            return;
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < Math.min(confusionMatrix.length, topK); i++) {
            for (int j = 0; j < Math.min(confusionMatrix[i].length, topK); j++) {
                dataset.addValue(confusionMatrix[i][j], "Predicted Rank " + i, "True Rank " + j);
            }
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Confusion Matrix",
                "True Rank",
                "Count",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Confusion Matrix", chartPanel);
    }

    /**
     * Visualizes query results distances.
     */
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
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Query Results", chartPanel);
    }

    /**
     * Visualizes raw data (2D projection for high-dimensional data).
     */
    public static void visualizeRawData(List<double[]> vectors, int dim, String datasetName) {
        if (vectors == null || vectors.isEmpty()) {
            logger.warn("No raw data to visualize for dataset {}", datasetName);
            return;
        }

        XYSeries series = new XYSeries("Raw Data");
        for (double[] vec : vectors) {
            series.add(vec[0], dim > 1 ? vec[1] : 0.0);
        }

        XYSeriesCollection dataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createScatterPlot(
                "Raw Data - " + datasetName,
                "Dimension 1",
                dim > 1 ? "Dimension 2" : "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Raw Data - " + datasetName, chartPanel);
    }

    /**
     * Visualizes indexed data.
     */
    public static void visualizeIndexedData(List<double[]> vectors, int dim, String datasetName) {
        if (vectors == null || vectors.isEmpty()) {
            logger.warn("No indexed data to visualize for dataset {}", datasetName);
            return;
        }

        XYSeries series = new XYSeries("Indexed Data");
        for (double[] vec : vectors) {
            series.add(vec[0], dim > 1 ? vec[1] : 0.0);
        }

        XYSeriesCollection dataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createScatterPlot(
                "Indexed Data - " + datasetName,
                "Dimension 1",
                dim > 1 ? "Dimension 2" : "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Indexed Data - " + datasetName, chartPanel);
    }

    /**
     * Visualizes fake points vs real data.
     */
    public static void visualizeFakePoints(List<double[]> fakePoints, List<double[]> realData, int dim) {
        if (fakePoints == null || fakePoints.isEmpty()) {
            logger.warn("No fake points to visualize");
            return;
        }

        XYSeries fakeSeries = new XYSeries("Fake Points");
        XYSeries realSeries = new XYSeries("Real Data");
        for (double[] vec : fakePoints) {
            fakeSeries.add(vec[0], dim > 1 ? vec[1] : 0.0);
        }
        for (double[] vec : realData) {
            realSeries.add(vec[0], dim > 1 ? vec[1] : 0.0);
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(fakeSeries);
        dataset.addSeries(realSeries);
        JFreeChart chart = ChartFactory.createScatterPlot(
                "Fake vs Real Points",
                "Dimension 1",
                dim > 1 ? "Dimension 2" : "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Fake vs Real Points", chartPanel);
    }

    /**
     * Visualizes encryption keys.
     */
    public static void visualizeEncryptionKeys(Map<String, byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            logger.warn("No encryption keys to visualize");
            return;
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        int index = 0;
        for (Map.Entry<String, byte[]> entry : keys.entrySet()) {
            dataset.addValue(entry.getValue().length, "Key Length", "Key " + index++);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Encryption Key Lengths",
                "Key",
                "Length (bytes)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Encryption Keys", chartPanel);
    }

    /**
     * Visualizes k-nearest neighbors.
     */
    public static void visualizeKNeighbors(List<QueryResult> results, int topK, int dim) {
        if (results == null || results.isEmpty()) {
            logger.warn("No k-neighbor results to visualize");
            return;
        }

        XYSeries series = new XYSeries("K-Neighbors");
        for (QueryResult result : results) {
            series.add(result.getDistance(), 0.0);
        }

        XYSeriesCollection dataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createScatterPlot(
                "K-Nearest Neighbors (Top " + topK + ")",
                "Distance",
                "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("K-Neighbors", chartPanel);
    }

    /**
     * Visualizes comparison of multiple datasets.
     */
    public static void visualizeDatasetComparison(List<List<double[]>> datasets, List<String> datasetNames, int dim) {
        if (datasets == null || datasets.isEmpty() || datasetNames == null || datasetNames.isEmpty()) {
            logger.warn("No datasets to compare");
            return;
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        for (int i = 0; i < datasets.size(); i++) {
            XYSeries series = new XYSeries(datasetNames.get(i));
            for (double[] vec : datasets.get(i)) {
                series.add(vec[0], dim > 1 ? vec[1] : 0.0);
            }
            dataset.addSeries(series);
        }

        JFreeChart chart = ChartFactory.createScatterPlot(
                "Dataset Comparison",
                "Dimension 1",
                dim > 1 ? "Dimension 2" : "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800, 600));
        displayInTab("Dataset Comparison", chartPanel);
    }

    /**
     * Displays a chart in a tabbed pane.
     */
    private static void displayInTab(String tabName, ChartPanel chartPanel) {
        JFrame frame = getOrCreateFrame();
        JTabbedPane tabbedPane = (JTabbedPane) frame.getContentPane().getComponent(0);

        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            if (tabbedPane.getTitleAt(i).equals(tabName)) {
                tabbedPane.remove(i);
                break;
            }
        }

        tabbedPane.add(tabName, chartPanel);
        frame.pack();
        frame.setVisible(true);
    }

    /**
     * Gets or creates the singleton JFrame with a tabbed pane.
     */
    private static JFrame getOrCreateFrame() {
        JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(JOptionPane.getRootFrame());
        if (frame == null || !frame.isDisplayable()) {
            frame = new JFrame("Performance Visualizations");
            frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            JTabbedPane tabbedPane = new JTabbedPane();
            frame.getContentPane().add(tabbedPane);
            frame.setSize(850, 650);
        }
        return frame;
    }
}