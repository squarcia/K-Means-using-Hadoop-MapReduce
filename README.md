# Hadoop MapReduce Java Program - Distributed Computing 🚀🔢

This Java program leverages the power of Hadoop MapReduce to process datasets efficiently. The implementation is designed to run on a Hadoop cluster consisting of three virtual machines, showcasing the capabilities of distributed computing.

## Usage

Follow the steps below to run the program:

1. **Build the Java Program with Maven**
    ```bash
    mvn clean package
    ```

2. **Configure Parameters**
    Set the necessary parameters in the configuration file `config.ini`.

3. **Copy Dataset to Hadoop Distributed File System (HDFS)**
    Transfer your dataset to the specified directory in the Hadoop Distributed File System (HDFS).

4. **Run the Jar File on the Hadoop Cluster**
    Execute the jar file, including dependencies, on your Hadoop cluster:
    ```bash
    hadoop jar target/kMeans-1.0-SNAPSHOT-with_dependencies.jar it.unipi.hadoop.kMeans
    ```
