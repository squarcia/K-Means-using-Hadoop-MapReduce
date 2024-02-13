package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;

/**
 * Main class for the k-Means algorithm implementation.
 */
public class kMeans {

    static Configuration conf;
    static FileSystem fs;
    static int maxNumberOfIterations;
    static double errorThreshold;

    /**
     * Cleans the Hadoop workspace by deleting intermediate and convergence folders.
     * @throws IOException
     */
    public static void cleanWorkspace() throws IOException {
        fs.delete(new Path(conf.get("finalMeans")), true);
        fs.delete(new Path(conf.get("convergence")), true);
    }

    /**
     * Copies files from the source path to the destination path.
     * @param srcPath Source path.
     * @param dstPath Destination path.
     * @throws IOException
     */
    public static void copy(Path srcPath, Path dstPath) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(srcPath, true);
        while (fileIter.hasNext()){
            FileUtil.copy(fs, fileIter.next(), fs, dstPath, false, true, conf);
        }
    }

    /**
     * Adds files from a directory to the Hadoop Distributed Cache for a job.
     * @param dir Directory path.
     * @param job Hadoop job instance.
     * @throws IOException
     */
    public static void addCacheDirectory(Path dir, Job job) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(dir, true);
        while(fileIter.hasNext()) {
            job.addCacheFile(fileIter.next().getPath().toUri());
        }
    }

    /**
     * Retrieves the objective function value from the convergence output.
     * @return Objective function value.
     * @throws IOException
     */
    public static double getObjectiveFunction() throws IOException {
        double objFunction;
        InputStream is = fs.open(new Path(conf.get("convergence") + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        if ((line = br.readLine()) == null ) {
            br.close();
            fs.close();
            System.exit(1);
        }

        objFunction = Double.parseDouble(line);
        br.close();
        return objFunction;
    }

    /**
     * Main method for running the k-Means algorithm.
     * @param args Command-line arguments.
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        initializeConfiguration();
        displayConfiguration();

        Job sampling = Job.getInstance(conf, "sampling means");
        if (!Sampling.main(sampling)) {
            fs.close();
            System.exit(1);
        }

        int step = 0;
        double objFunction = Double.POSITIVE_INFINITY;
        double prevObjFunction;
        double variation;

        do {
            prevObjFunction = objFunction;

            Path srcPath = (step == 0) ? new Path(conf.get("sampledMeans")) : new Path(conf.get("finalMeans"));
            Path dstPath = new Path(conf.get("intermediateMeans"));

            fs.mkdirs(dstPath);
            copy(srcPath, dstPath);

            cleanWorkspace();

            runClusteringJob();

            runConvergenceJob();

            objFunction = getObjectiveFunction();

            variation = calculateVariation(prevObjFunction, objFunction);

            printStepSummary(step, prevObjFunction, objFunction, variation);

            step++;
        } while (prevObjFunction == Double.POSITIVE_INFINITY || (variation > errorThreshold && step < maxNumberOfIterations));

        fs.close();
    }

    /**
     * Calculates the percentage variation between two values.
     * @param prevValue Previous value.
     * @param currentValue Current value.
     * @return Percentage variation.
     */
    private static double calculateVariation(double prevValue, double currentValue) {
        if (prevValue == Double.POSITIVE_INFINITY) {
            return Double.POSITIVE_INFINITY;
        } else {
            return ((prevValue - currentValue) / prevValue) * 100;
        }
    }

    /**
     * Prints a summary of the current step including the objective function values and variation.
     * @param step Current iteration step.
     * @param prevObjFunction Previous objective function value.
     * @param objFunction Current objective function value.
     * @param variation Percentage variation.
     */
    private static void printStepSummary(int step, double prevObjFunction, double objFunction, double variation) {
        System.out.println("╔════════════════════════════════════╗");
        System.out.printf("║ RESULT                             ║\n");
        System.out.println("╠════════════════════════════════════╣");
        System.out.printf("║ PREV_OBJ_FUNCTION: %-12.2f    ║\n", prevObjFunction);
        System.out.printf("║ OBJ_FUNCTION:      %-12.2f    ║\n", objFunction);
        System.out.printf("║ DELTA:             %-12.2f%%   ║\n", variation);
        System.out.println("╚════════════════════════════════════╝\n");
    }

    /**
     * Initializes Hadoop configuration and the file system.
     * @throws IOException
     */
    private static void initializeConfiguration() throws IOException {
        conf = new Configuration();
        CustomConfiguration localConfig = new CustomConfiguration("config.yaml");
        localConfig.displayConfiguration();

        maxNumberOfIterations = localConfig.getMaxIterations();
        errorThreshold = localConfig.getConvergenceThreshold();

        String BASE_DIR = localConfig.getOutputFilePath() + "/";
        conf.setLong("seed", localConfig.getRandomSeed());
        conf.setInt("d", localConfig.getDimensionsCount());
        conf.setInt("k", localConfig.getClustersCount());
        conf.setInt("maxNumberOfReduceTasks", localConfig.getNumberOfReducers());
        conf.set("input", localConfig.getInputFilePath());
        conf.set("sampledMeans", BASE_DIR + "sampled-means");
        conf.set("intermediateMeans", BASE_DIR + "intermediate-means");
        conf.set("finalMeans", BASE_DIR + "final-means");
        conf.set("convergence", BASE_DIR + "convergence");

        fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);
    }

    /**
     * Displays the current configuration details.
     */
    private static void displayConfiguration() {
        System.out.println("Current Configuration:");
        System.out.println("Random Seed: " + conf.getLong("seed", 0));
        System.out.println("Dimensions Count (d): " + conf.getInt("d", 2));
        System.out.println("Clusters Count (k): " + conf.getInt("k", 2));
        System.out.println("Max Number of Reduce Tasks: " + conf.getInt("maxNumberOfReduceTasks", 1));
        System.out.println("Input File Path: " + conf.get("input"));
        System.out.println("Sampled Means Output Path: " + conf.get("sampledMeans"));
        System.out.println("Intermediate Means Output Path: " + conf.get("intermediateMeans"));
        System.out.println("Final Means Output Path: " + conf.get("finalMeans"));
        System.out.println("Convergence Output Path: " + conf.get("convergence"));
        System.out.println();
    }

    /**
     * Runs the clustering job.
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private static void runClusteringJob() throws IOException, InterruptedException, ClassNotFoundException {
        Job clustering = Job.getInstance(conf, "clustering");
        addCacheDirectory(new Path(conf.get("intermediateMeans")), clustering);
        if (!Clustering.main(clustering)) {
            fs.close();
            System.exit(1);
        }
    }

    /**
     * Runs the convergence job.
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private static void runConvergenceJob() throws IOException, InterruptedException, ClassNotFoundException {
        Job convergence = Job.getInstance(conf, "convergence");
        addCacheDirectory(new Path(conf.get("finalMeans")), convergence);
        if (!Convergence.main(convergence)) {
            fs.close();
            System.exit(1);
        }
    }
}
