package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.stream.StreamSupport;

/**
 * The main class for the convergence job.
 */
public class Convergence {

    /**
     * Mapper class for the convergence job.
     */
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        final static List<Point> means = new ArrayList<>();
        final static Text outputKey = new Text();
        final static DoubleWritable outputValue = new DoubleWritable();
        static double distanceAccumulator;

        /**
         * Setup method to initialize the mapper with means loaded from cache files.
         */
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distanceAccumulator = 0.0;
            means.clear();

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new InterruptedException("Cache files not found. Ensure that means file is distributed.");
            }

            try {
                FileSystem fs = FileSystem.get(conf);

                for (URI cacheFile : cacheFiles) {
                    Path filePath = new Path(cacheFile);

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            Point mean = new Point(line);
                            means.add(mean);
                        }
                    } catch (IOException e) {
                        // Log the error and continue with the next file
                        context.getCounter("Setup", "FileReadError").increment(1);
                        System.err.println("Error reading file from cache: " + filePath.toString());
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                // Log the error for FileSystem initialization failure
                context.getCounter("Setup", "FileSystemInitError").increment(1);
                System.err.println("Error initializing FileSystem.");
                e.printStackTrace();
            }

            if (means.isEmpty()) {
                // Log a warning if no means are loaded
                context.getCounter("Setup", "NoMeansFound").increment(1);
                System.out.println("Warning: No means loaded from cache files. Ensure cache files contain valid means.");
            }
        }

        /**
         * Map function processes each input point and accumulates the distance to the closest mean.
         */
        public void map(LongWritable key, Text value, Context context) {
            // Create a Point object from the input Text value
            Point p = new Point(value.toString());

            // Find the closest mean using Java 8 stream and compute the distance
            double minDistance = means.stream()
                    .mapToDouble(m -> p.computeDistance(m))
                    .min()
                    .orElse(Double.POSITIVE_INFINITY);

            // Update the distance accumulator with the minimum distance
            distanceAccumulator += minDistance;
        }

        /**
         * Cleanup function to emit the sum of distances using a common key for the reducer.
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Use a descriptive key for the output
            Text outputKey = new Text("distance_sum");

            // Set the distance accumulator as the output value
            DoubleWritable outputValue = new DoubleWritable(distanceAccumulator);

            // Write the key-value pair to the output
            context.write(outputKey, outputValue);
        }
    }

    /**
     * Reducer class for the convergence job.
     */
    public static class ConvergenceReducer extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {

        static double objFunction;
        final static DoubleWritable outputValue = new DoubleWritable();

        /**
         * Reduce function sums up all the distances received from the mappers.
         */
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // Calculate the sum of distances using Java streams
            double objFunction = StreamSupport.stream(values.spliterator(), false)
                    .mapToDouble(DoubleWritable::get)
                    .sum();

            // Set the calculated sum as the output value
            DoubleWritable outputValue = new DoubleWritable(objFunction);

            // Write the key-value pair to the output
            context.write(null, outputValue);
        }
    }

    /**
     * Main method to configure and run the convergence job.
     */
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(Convergence.class);

        // Set Mapper class
        job.setMapperClass(ConvergenceMapper.class);

        // Set Reducer class
        job.setReducerClass(ConvergenceReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("convergence")));

        // Run the job and return the status
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}
