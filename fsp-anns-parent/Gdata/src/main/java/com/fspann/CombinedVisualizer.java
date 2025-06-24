package com.fspann;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class CombinedVisualizer {

    public static void main(String[] args) throws IOException {
        // Folder containing CSV files
        String folderPath = "datasetsMillion/storage";

        // Target dimensions to plot
        int[] dimsToPlot = {0, 1, 10};

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folderPath), "*.csv")) {
            for (Path entry : stream) {
                String filePath = entry.toString();
                System.out.println("Processing: " + filePath);

                for (int dim : dimsToPlot) {
                    List<Double> values = loadDimensionValues(filePath, dim);

                    Histogram histogram = new Histogram(values, 50); // 50 bins

                    CategoryChart chart = new CategoryChartBuilder()
                            .width(1000).height(600)
                            .title("Histogram + Curve of dimension " + dim + " : " + Paths.get(filePath).getFileName())
                            .xAxisTitle("Value").yAxisTitle("Count")
                            .build();

                    // Chart styling
                    chart.getStyler().setLegendVisible(true);
                    chart.getStyler().setPlotBackgroundColor(java.awt.Color.WHITE);
                    chart.getStyler().setChartBackgroundColor(java.awt.Color.WHITE);
                    chart.getStyler().setPlotGridLinesVisible(false);
                    chart.getStyler().setXAxisLabelRotation(45);

                    chart.addSeries("Histogram", histogram.getxAxisData(), histogram.getyAxisData());
                    chart.addSeries("Curve", histogram.getxAxisData(), histogram.getyAxisData());

                    String outFile = filePath + ".dim" + dim + ".combined.png";
                    BitmapEncoder.saveBitmap(chart, outFile, BitmapEncoder.BitmapFormat.PNG);
                    System.out.println("✅ Combined chart saved: " + outFile);
                }
            }
        }
    }

    private static List<Double> loadDimensionValues(String csvFile, int dimIndex) throws IOException {
        List<Double> values = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(csvFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                if (dimIndex < tokens.length) {
                    double v = Double.parseDouble(tokens[dimIndex]);
                    values.add(v);
                }
            }
        }

        System.out.println("Loaded " + values.size() + " values from " + csvFile + " dimension " + dimIndex);
        return values;
    }
}
