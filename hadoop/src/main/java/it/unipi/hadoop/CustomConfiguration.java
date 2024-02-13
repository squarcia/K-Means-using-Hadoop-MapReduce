package it.unipi.hadoop;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;

/**
 * The class responsible for parsing configuration parameters from a YAML file.
 */
public class CustomConfiguration {
    private int dimensionsCount;
    private int clustersCount;
    private String inputFilePath;
    private String outputFilePath;
    private int randomSeed;
    private int numberOfReducers;
    private double convergenceThreshold;
    private int maxIterations;

    /**
     * Constructor that reads the configuration from a YAML file.
     * @param configFilePath Path to the YAML configuration file.
     */
    public CustomConfiguration(String configFilePath) {

        try (InputStream input = Files.newInputStream(Paths.get(configFilePath))) {
            Yaml yaml = new Yaml();
            Map<String, Object> yamlData = yaml.load(input);

            Map<String, Object> yamlDataset = (Map<String, Object>) yamlData.get("Dataset");
            dimensionsCount = getIntValue(yamlDataset, "dimensionsCount");
            clustersCount = getIntValue(yamlDataset, "clustersCount");
            inputFilePath = getStringValue(yamlDataset, "inputFilePath");
            outputFilePath = getStringValue(yamlDataset, "outputFilePath");

            Map<String, Object> yamlKMeans = (Map<String, Object>) yamlData.get("KMeans");
            randomSeed = getIntValue(yamlKMeans, "randomSeed");
            numberOfReducers = getIntValue(yamlKMeans, "numberOfReducers");
            convergenceThreshold = getDoubleValue(yamlKMeans, "convergenceThreshold");
            maxIterations = getIntValue(yamlKMeans, "maxIterations");

            validate();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Validate configuration parameters.
     */
    private void validate() {
        if (dimensionsCount <= 0 || clustersCount <= 0 || numberOfReducers <= 0 || maxIterations <= 0) {
            System.err.println("Invalid configuration: Ensure dimensions, clusters, reducers, and iterations are greater than 0");
            System.exit(1);
        }

        if (convergenceThreshold < 0) {
            System.err.println("Invalid configuration: Convergence threshold must be greater than or equal to 0");
            System.exit(1);
        }
    }

    /**
     * Get an integer value from the YAML map.
     * @param map YAML map.
     * @param key Key for the value.
     * @return Integer value.
     */
    private int getIntValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return (value instanceof Integer) ? (int) value : 0;
    }

    /**
     * Get a string value from the YAML map.
     * @param map YAML map.
     * @param key Key for the value.
     * @return String value.
     */
    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return (value instanceof String) ? (String) value : "";
    }

    /**
     * Get a double value from the YAML map.
     * @param map YAML map.
     * @param key Key for the value.
     * @return Double value.
     */
    private double getDoubleValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return (value instanceof Double) ? (double) value : 0.0;
    }

    /**
     * Get the number of dimensions.
     * @return Number of dimensions.
     */
    public int getDimensionsCount() {
        return dimensionsCount;
    }

    /**
     * Get the number of clusters.
     * @return Number of clusters.
     */
    public int getClustersCount() {
        return clustersCount;
    }

    /**
     * Get the input file path.
     * @return Input file path.
     */
    public String getInputFilePath() {
        return inputFilePath;
    }

    /**
     * Get the output file path.
     * @return Output file path.
     */
    public String getOutputFilePath() {
        return outputFilePath;
    }

    /**
     * Get the random seed.
     * @return Random seed.
     */
    public int getRandomSeed() {
        return randomSeed;
    }

    /**
     * Get the number of reducers.
     * @return Number of reducers.
     */
    public int getNumberOfReducers() {
        return numberOfReducers;
    }

    /**
     * Get the convergence threshold.
     * @return Convergence threshold.
     */
    public double getConvergenceThreshold() {
        return convergenceThreshold;
    }

    /**
     * Get the maximum number of iterations.
     * @return Maximum number of iterations.
     */
    public int getMaxIterations() {
        return maxIterations;
    }

    /**
     * Display the configuration details.
     */
    public void displayConfiguration() {
        System.out.println("----------------------------------------------------");
        System.out.printf("| %-25s | %-30s |\n", "Configuration Parameter", "Value");
        System.out.println("----------------------------------------------------");
        printTableRow("Number of Dimensions", String.valueOf(dimensionsCount));
        printTableRow("Number of Clusters", String.valueOf(clustersCount));
        printTableRow("Input File Path", inputFilePath);
        printTableRow("Output File Path", outputFilePath);
        printTableRow("Random Seed", String.valueOf(randomSeed));
        printTableRow("Number of Reducers", String.valueOf(numberOfReducers));
        printTableRow("Convergence Threshold", String.format("%.2f%%", convergenceThreshold));
        printTableRow("Maximum Iterations", String.valueOf(maxIterations));
        System.out.println("----------------------------------------------------");
        System.out.println();
    }

    /**
     * Helper method to print a table row.
     * @param parameter Parameter name.
     * @param value Parameter value.
     */
    private void printTableRow(String parameter, String value) {
        System.out.printf("| %-25s | %-30s |\n", parameter, value);
    }
}
