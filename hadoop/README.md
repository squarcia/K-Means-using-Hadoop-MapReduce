# _k_-means Hadoop Implementation 🌐🔢

1. 🛠️ **Build the Package with Maven**

    ```bash
    mvn clean package
    ```

2. 📝 **Configure Parameters in `config.ini`**

    Set the required parameters in the configuration file `config.ini`.

3. 📂 **Copy Dataset to Hadoop Distributed File System (HDFS)**

    Copy the dataset to the specified directory in the Hadoop Distributed File System (HDFS).

4. 🚀 **Run the Jar File with Dependencies**

    Execute the jar file **with dependencies** using the following command:

    ```bash
    hadoop jar target/kMeans-1.0-SNAPSHOT-with_dependencies.jar it.unipi.hadoop.kMeans
    ```
