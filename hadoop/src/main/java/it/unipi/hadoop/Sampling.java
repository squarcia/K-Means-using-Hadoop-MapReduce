package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * The Sampling class implements the sampling phase of the k-means algorithm.
 */
public class Sampling {
    static int numberOfCentroids;

    public static class SamplingMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        final static Random rand = new Random();
        final static IntWritable outputKey = new IntWritable();
        final static Point outputValue = new Point();
        final static PriorityQueue<PriorityPoint> pq = new PriorityQueue<>();

        /**
         * The setup function is responsible for configuring the Mapper's environment
         * before the actual map process. It introduces the possibility to handle additional configurations
         * and establishes the necessary initial conditions for the Mapper to work correctly.
         * @param context Mapper's context.
         */
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            String additionalConfig = conf.get("additionalConfig");
            if (additionalConfig != null) {
                System.out.println("Additional Configuration: " + additionalConfig);
            }

            numberOfCentroids = Integer.parseInt(conf.get("k"));
            if (numberOfCentroids <= 0) {
                throw new IllegalArgumentException("Number of centroids must be positive");
            }

            rand.setSeed(Long.parseLong(conf.get("seed")));
        }

        /**
         * The cleanup function performs cleanup operations, exception handling, and resource closing
         * before the Mapper completely terminates.
         * @param context Mapper's context.
         */
        public void cleanup(Context context) {
            try {
                for (PriorityPoint pp : pq)
                    preprocessAndWrite(pp, context);

            } catch (Exception e) {
                handleCleanupException(e);
            }
        }

        /**
         * The preprocessAndWrite function prepares and writes a single point (PriorityPoint) to the Mapper's context,
         * using the key and values associated with the PriorityPoint object.
         * @param point PriorityPoint to be processed.
         * @param context Mapper's context.
         * @throws IOException
         * @throws InterruptedException
         */
        private void preprocessAndWrite(PriorityPoint point, Context context) throws IOException, InterruptedException {
            outputKey.set(point.getPriority());
            outputValue.setCoordinates(point.getCoordinates());
            context.write(outputKey, outputValue);
        }

        private void handleCleanupException(Exception e) {
            // Implement exception handling in cleanup
            System.err.println("Error during cleanup:");
            e.printStackTrace();
        }

        /**
         * The map function adds PriorityPoints to the priority queue, keeping the queue size up to the number of centroids.
         * @param key Input key.
         * @param value Input value.
         * @param context Mapper's context.
         */
        public void map(LongWritable key, Text value, Context context) {
            pq.add(new PriorityPoint(rand.nextInt(), value.toString()));

            // Keep the queue size up to the number of centroids
            if (pq.size() > numberOfCentroids)
                pq.poll();
        }
    }

    public static class SamplingReducer extends Reducer<IntWritable, Point, NullWritable, Point> {
        static int meansCount;

        /**
         * The setup function processes configuration parameters before the Reducer's execution.
         * @param context Reducer's context.
         */
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                processConfigurationParameters(conf);

            } catch (Exception e) {
                handleSetupException(e);
            }
        }

        private void processConfigurationParameters(Configuration conf) {
            String additionalParam = conf.get("additionalParam");
            if (additionalParam != null) {
                System.out.println("Additional Parameter: " + additionalParam);
            }
        }

        private void handleSetupException(Exception e) {
            System.err.println("Error during setup:");
            e.printStackTrace();
        }

        private void handleReduceException(Exception e) {
            System.err.println("Error during reduce:");
            e.printStackTrace();
        }

        /**
         * The reduce function processes and prints information for each reduced point,
         * checks the maximum number of centroids, and writes the points to the output context.
         * Any exceptions during execution are handled by the handleReduceException function.
         * @param key Input key.
         * @param values Iterable of input values.
         * @param context Reducer's context.
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            try {
                for (Point p : values) {
                    System.out.println("Processing Reduced Point: " + p.toString());

                    if (meansCount > numberOfCentroids) {
                        // Example of special action when exceeding the maximum number of centroids
                        System.out.println("Exceeded Maximum Centroids. Performing Special Action");
                        return;
                    }

                    context.write(NullWritable.get(), p);
                    meansCount++;
                }
            } catch (Exception e) {
                handleReduceException(e);
            }
        }
    }

    /**
     * The main function sets up and runs the MapReduce job for the Sampling phase.
     * @param job MapReduce job.
     * @return True if the job is successful, false otherwise.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        job.setJarByClass(Sampling.class);

        job.setMapperClass(SamplingMapper.class);
        job.setReducerClass(SamplingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("sampledMeans")));

        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}
