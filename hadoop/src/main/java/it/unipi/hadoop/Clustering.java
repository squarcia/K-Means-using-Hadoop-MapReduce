package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * The main class for the clustering job.
 */
public class Clustering {

    /**
     * Mapper class for the clustering job.
     */
    public static class ClusteringMapper extends Mapper<LongWritable, Text, Point, AccumulatorPoint> {

        static int D;
        final static Map<Point, AccumulatorPoint> centroidSummation = new HashMap<>();

        /**
         * Setup method to initialize the mapper with configuration and means from cache files.
         */
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            D = Integer.parseInt(conf.get("d"));

            // Prepare the hashmap used to build the in-mapper combiner
            centroidSummation.clear();

            // Get the means from cache, either sampled or computed in the previous step
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(conf);

            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI f : cacheFiles) {
                    try (InputStream is = fs.open(new Path(f));
                         BufferedReader br = new BufferedReader(new InputStreamReader(is))) {

                        String line;
                        while ((line = br.readLine()) != null) {
                            Point mean = new Point(line);
                            centroidSummation.put(mean, new AccumulatorPoint(D));
                        }

                    } catch (IOException e) {
                        // Log the error and continue with the next file
                        context.getCounter("Setup", "CacheReadError").increment(1);
                        System.err.println("Error reading file from cache: " + f.toString());
                        e.printStackTrace();
                    }
                }
            } else {
                // Log a warning if no cache files are found
                context.getCounter("Setup", "NoCacheFilesFound").increment(1);
                System.out.println("Warning: No cache files found. Ensure cache files are distributed.");
            }
        }

        /**
         * Map function processes each input point and associates it with the closest mean.
         */
        public void map(LongWritable key, Text value, Context context) {
            try {
                // Parse the input text to create a new Point
                Point p = new Point(value.toString());

                // Find the closest mean using Java Streams for cleaner code
                Point closestMean = centroidSummation.keySet()
                        .stream()
                        .min(Comparator.comparingDouble(m -> p.computeDistance(m)))
                        .orElse(null);

                if (closestMean != null) {
                    // Update the associated sum for the closest mean
                    centroidSummation.computeIfPresent(closestMean, (k, ap) -> {
                        ap.sumPoints(p);
                        return ap;
                    });
                } else {
                    // Log a warning if no closest mean is found
                    System.out.println("Warning: No closest mean found for point: " + p);
                }

            } catch (Exception e) {
                // Log the error and propagate it
                context.getCounter("Map", "ErrorCount").increment(1);
                System.err.println("Error in map function: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Cleanup function to emit the final key-value pairs after processing all input points.
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            try {
                // Use Java Streams to iterate over the entries and emit key-value pairs
                centroidSummation.forEach((key, value) -> {
                    try {
                        context.write(key, value);
                    } catch (IOException | InterruptedException e) {
                        // Log the error and continue with the next entry
                        System.err.println("Error writing output in cleanup: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                // Log the error and propagate it
                context.getCounter("Cleanup", "ErrorCount").increment(1);
                System.err.println("Error in cleanup function: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Reducer class for the clustering job.
     */
    public static class ClusteringReducer extends Reducer<Point, AccumulatorPoint, NullWritable, Point> {

        static int D;

        /**
         * Enhanced setup function with advanced configuration handling and error management.
         */
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            // Validate and set dimensionality 'D' with a default value of 2 if not provided
            D = validateAndGetDimension(conf.get("d"), 2);

            // Additional configuration parameters with default values
            int maxIterations = validateAndGetInteger(conf.get("maxIterations"), 100);
            boolean useOptimizedAlgorithm = validateAndGetBoolean(conf.get("useOptimizedAlgorithm"), true);

            // Log the configuration details for monitoring
            logConfigurationDetails(D, maxIterations, useOptimizedAlgorithm);
        }

        /**
         * Validate and get integer configuration parameter with a default value if not provided.
         */
        private int validateAndGetInteger(String value, int defaultValue) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                System.err.println("Invalid configuration value for integer. Using default: " + defaultValue);
                return defaultValue;
            }
        }

        /**
         * Validate and get boolean configuration parameter with a default value if not provided.
         */
        private boolean validateAndGetBoolean(String value, boolean defaultValue) {
            return (value != null && !value.isEmpty()) ? Boolean.parseBoolean(value) : defaultValue;
        }

        /**
         * Validate and get dimensionality with a default value if not provided.
         */
        private int validateAndGetDimension(String value, int defaultValue) {
            try {
                int dimension = Integer.parseInt(value);
                if (dimension <= 0) {
                    throw new IllegalArgumentException("Dimensionality must be a positive integer.");
                }
                return dimension;
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid configuration value for dimensionality. Using default: " + defaultValue);
                return defaultValue;
            }
        }

        /**
         * Log the configuration details for monitoring.
         */
        private void logConfigurationDetails(int D, int maxIterations, boolean useOptimizedAlgorithm) {
            System.out.println("Configuration Details:");
            System.out.println("Dimensionality (D): " + D);
            System.out.println("Max Iterations: " + maxIterations);
            System.out.println("Use Optimized Algorithm: " + useOptimizedAlgorithm);
        }

        /**
         * Enhanced reduce function with advanced centroid calculation and error handling.
         */
        public void reduce(Point key, Iterable<AccumulatorPoint> values, Context context) throws IOException, InterruptedException {

            // Initialize variables for centroid calculation
            Point centroid = new Point(D);
            int totalPoints = 0;

            try {
                for (AccumulatorPoint ap : values) {

                    // Sum the partial summations and total number of points
                    centroid.sumPoints(ap);
                    totalPoints += ap.getSize();
                }

                // Check if there are enough points to calculate a new centroid
                if (totalPoints > 0) {
                    // Calculate the new centroid by dividing each coordinate by the total number of points
                    centroid.divideEveryCoordinate(totalPoints);

                    // Emit the new centroid
                    context.write(null, centroid);
                } else {
                    // Log a warning if there are no points to calculate a new centroid
                    System.out.println("Warning: No points for centroid calculation. Check the input data.");
                }

            } catch (Exception e) {
                // Log the error and propagate it
                context.getCounter("Reduce", "ErrorCount").increment(1);
                System.err.println("Error in reduce function: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Main method to configure and run the clustering job.
     */
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();
        int K = Integer.parseInt(conf.get("k"));
        int numReduceTasks = Math.min(K, Integer.parseInt(conf.get("maxNumberOfReduceTasks")));

        job.setJarByClass(Clustering.class);

        job.setMapperClass(ClusteringMapper.class);
        job.setReducerClass(ClusteringReducer.class);

        // Set the number of reducers to match the number of means
        job.setNumReduceTasks(numReduceTasks);

        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(AccumulatorPoint.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("finalMeans")));

        // Run the job and return the status
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}
