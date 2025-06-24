package com.fspann;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class DatasetVisualizer {

    public static void main(String[] args) throws IOException {
        String filePath = "datasetsMillion/storage/synthetic_uniform_1024d.csv"; // example
        int targetDimension = 1023;

        List<Double> values = loadDimensionValues(filePath, targetDimension);

        Histogram histogram = new Histogram(values, 30); // 30 bins

        CategoryChart chart = new CategoryChartBuilder()
                .width(1000).height(600)
                .title("Histogram of dimension " + targetDimension + " : " + Paths.get(filePath).getFileName())
                .xAxisTitle("Value").yAxisTitle("Count")
                .build();

        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setPlotBackgroundColor(java.awt.Color.WHITE);
        chart.getStyler().setChartBackgroundColor(java.awt.Color.WHITE);
        chart.getStyler().setPlotGridLinesVisible(false);
        chart.getStyler().setXAxisLabelRotation(45);

        chart.addSeries("Frequency", histogram.getxAxisData(), histogram.getyAxisData());

        String outFile = filePath + ".dim" + targetDimension + ".png";
        BitmapEncoder.saveBitmap(chart, outFile, BitmapEncoder.BitmapFormat.PNG);
        System.out.println("✅ Chart saved: " + outFile);
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

        System.out.println("Loaded " + values.size() + " values from " + csvFile);
        return values;
    }
}
