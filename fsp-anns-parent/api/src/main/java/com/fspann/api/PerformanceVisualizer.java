package com.fspann.api;

import com.fspann.common.QueryResult;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.util.List;
import java.util.Map;

/**
 * Visualizes performance metrics and query results using JFreeChart.
 */
public class PerformanceVisualizer {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceVisualizer.class);

    /**
     * Visualizes a list of timing measurements (e.g., from Profiler).
     * @param timings List of timing values in nanoseconds
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
                dataset
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        displayInTab("Timings", chartPanel);
    }

    /**
     * Visualizes a confusion matrix for query accuracy.
     * @param confusionMatrix 2D array where rows are predicted ranks, columns are true ranks
     * @param topK Number of top neighbors considered
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
                dataset
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        displayInTab("Confusion Matrix", chartPanel);
    }

    /**
     * Visualizes query results distances.
     * @param results List of query results
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
                dataset
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        displayInTab("Query Results", chartPanel);
    }

    /**
     * Displays a chart in a tabbed pane (singleton JFrame).
     * @param tabName Name of the tab
     * @param chartPanel Chart panel to display
     */
    private static void displayInTab(String tabName, ChartPanel chartPanel) {
        JFrame frame = getOrCreateFrame();
        JTabbedPane tabbedPane = (JTabbedPane) frame.getContentPane().getComponent(0);

        // Remove existing tab with the same name if it exists
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
     * @return JFrame instance
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