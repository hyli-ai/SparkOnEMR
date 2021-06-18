# Amazon EMR lab
>
In this project, an EMR cluster with one master and one core node is created with spot instance. A dataset of 324 files in CSV format is loaded from S3, and a test query is run on both Spark<sup>[1](#myfootnote1)</sup>  and Hive<sup>[2](#myfootnote2)</sup>  to compare the results.

<a name="myfootnote1"><sup>1</sup></a> Apache Spark is a big data framework which contains libraries for data analysis, machine learning, graph analysis, and streaming live data. Spark is generally faster than Hadoop because while Hadoop writes intermediate results to disk, Spark keeps intermediate results in memory.

<a name="myfootnote2"><sup>2</sup></a> Apache Hive is a data warehouse software built on top of Apache Hadoop for providing data query and analysis using SQL-like query statements.

## Table of contents

* [Introduction](#introduction)
* [Dataset](#dataset)
* [Files in this Project](#files-in-this-project)
* [Project Instructions](#project-instructions)

## Introduction

### Scenario
You work for an organization that has a wide variety of users. You have been tasked with running some data analytics for an upcoming marketing campaign. The end goal is to determine the most common users, grouped by their gender and age.

You have all the data you need stored in an S3 bucket. In this lab, you will be in charge of running data analytics on hundreds/thousands of files containing CSV data about the users who interact with the application. To accomplish this, you will first need to create an EMR cluster and copy user data into HDFS. Next, you will run a PySpark Apache Spark script to count the number of users, grouping them by their age and gender. Finally, you will need to load the results into S3 for further analysis.

![flowchart](/images/flowchart.jpg)

### Goal
The goal of this project, is to present Amazon EMR from an operation point of view. This includes the following parts:

* Create and configure an EMR cluster

* Copy data from S3 to EMR using the `s3-dist-cp` command

* Query the data using five different methods:
    1. Run the PySpark script in a **Step** using `spark-submit`
    2. Run the PySpark script using Jupyter Notebook
    3. Run the PySpark script using spark-submit command
    4. Create a table and run the query in Hive
    5. Run the query in spark-sql

* Copy data from EMR back to S3 using the `s3-dist-cp` command

The focus of this project will be on how Amazon EMR works, how to load the data into HDFS, and run a query against the engines built on top of it. There is another project on building a data pipeline with Spark on Amazon EMR in [this](https://github.com/hyli-ai/dl-spark) repository.

## Dataset
The dataset presented here is the **user-data-acg** which contains 324 csv format files. It resides in S3, and here is the link:

* User data: `s3://das-c01-data-analytics-specialtyData_Analytics_With_Spark_and_EMR/`

## Files in this Project
In addition to the data files, the project workspace includes six files:

1. `emr-pyspark-code.py` is the script for Spark to run.

2. `spark_notebook.ipynb` has the same statement as `emr-pyspark-code.py` but in a Jupyter Notebook.

3. `hive-table.txt` has the statements for Hive to create the table, and the SQL statement to count the number of the users and group them by their age and gender.

4. `image` folder contains all the images for the README.md file.

5. `README.md` provides discussion on this project.

## Project Instructions

### Part A - Preparation
1. Log into the AWS console
2. This project is created in US-East-1 region
3. Since we will need to SSH into the master node, we need to go to EC2 and create a key pair.

    ![EC2_KP](/images/EC2_KP.png)

### Part B - Create an EMR cluster
1. Click on **Create cluster** and select **Go to advanced options**
2. Step 1: Software and Steps
    Under **Software Configuration**, select the following:  
    Release: emr-5.31.0<sup>[3](#myfootnote3)</sup>  
    Applications: Hadoop 2.10.1  
                  Hive 2.3.7  
                  Spark 2.4.7  
                  Hue 4.9.0 (optional)  
                  Pig 0.17.0 (optional)  

    <a name="myfootnote3"><sup>3</sup></a> The reason for me to choose an older version is, according to [Amazon EMR documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-considerations.html), "For EMR release versions 5.32.0 and later, or 6.2.0 and later, your cluster must also be running the Jupyter Enterprise Gateway application in order to work with EMR Notebooks."

3. Step 2: Hardware  
    On **Cluster Nodes and Instances** section, use the following settings:  
    We only need 1 Master node and 1 Core node, no Task node is required. The instance type is `m4.large`.  
    We can use spot instances to save cost in this project. Check the information icon, and set the max price per instance/hr $0.001 higher than current spot price.

    ![EMR_Hardware](/images/EMR_hardware_selection.png)

4. Step 3: General Cluster Settings  
    Cluster name: emr_lab

5. Step 4: Security
    We can select the key pair which has been created and downloaded, and create the cluster.

    ![EMR_KP](/images/EMR_KP.png)

6. It will take about 10 mins to finish the creation process. The master node and core node on the cluster overview will be in the **Running** state once the cluster is ready.

    ![Overview](/images/cluster_overview.png)

### Part C - Configuration
1. In order to access the Hadoop UI and Spark UI, we need to change the Security Group setting to open the port. On the **Application and interface** tab, check the port for the HDFS Name Node, which is 50070. The port for Saprk History Server is 18080.

    ![Application&Interface](/images/UI_url.png)

2. Switch back to **Summary** tab, select the Security Group of **ElasticMapReduce-master** under Security and access section, and add the following ports to the inbound rule:

    | Protocol | Port Range | Source | Note  |
    | :------: | :--------: | :----: | :---: |
    | TCP      | 50070      | My IP  | Hadoop UI | 
    | TCP      | 18080      | My IP  | Spark UI |
    | SSH      |    22      | My IP  | Master Node access |

    ![security_and_access](/images/security_and_access.png)

3. Open the HDFS Name Node User Interface URL in another tab in Chrome, we will be able to see the Hadoop overview:  
    ![hadoop_overview](/images/hadoop_overview.png)

4. If we select **Browse the file system** **Utilities** tab, we can see the folders and files in our HDFS:  
    ![hadoop_files](/images/hadoop_files.png)

5. We can also check the files in HDFS by SSH into the master node, and use the command: (see Part G and H for SSH details)

    ```
    hdfs dfs -ls /
    ```

### Part D - Copy data from S3 to EMR
1. Add a step to the EMR cluster with the following details:

    * Step type: Custom JAR
    * Name: Copy data and script to HDFS
    * JAR location: command-runner.jar<sup>[4](#myfootnote4)</sup>
    * Arguments:
        ```
        s3-dist-cp --src=s3://das-c01-data-analytics-specialty/Data_Analytics_With_Spark_and_EMR/ --dest=hdfs:///
        ```
    * Action on failure: Continue

    <a name="myfootnote4"><sup>4</sup></a> `command-runner.jar` contains [a list of scripts](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html) that can be executed by the EMR. Here we are using the `s3-dist-cp` command to copy large amounts of data from Amazon S3 into HDFS.

    ![copy_data_to_hdfs](/images/copy_data_to_hdfs.png)

2. When the step is complete, reload the files in the Hadoop cluster. Two folders are created, where the **user-data-acg** folder contains all the csv files for analysis, and the **pyspark-script** folder contains the python script to be used in the next step.

    ![hadoop_files_new](/images/hadoop_files_new.png)

### Part E - Run the PySpark script in a **Step** using `spark-submit`
1. This is the simplest (and fastest) method to run this query. Just add another step in the cluster: 

    * Step type: Custom JAR
    * Name: Run PySpark script
    * JAR location: command-runner.jar
    * Arguments:
        ```
        spark-submit hdfs:///pyspark-script/emr-pyspark-code.py
        ```
    * Action on failure: Continue

    ![run_pyspark_step](/images/run_pyspark_step.png)

2. Since there is a line in the script `results.show()` to tell Spark to show the first 20 rows of the query result, we can click on **stdout** after this step is complete:  
    ![stdout](/images/stdout.png)

3. Here is the partial result of the first query:  
    ![query_result](/images/query_result.png)

4. The complete query result will be saved in a folder named `results`.

### Part F - Run the PySpark script using Jupyter Notebook
1. This method give us the flexibility to do data wrangling and debug the script one step at a time.

2. First we need to create a Notebook.

3. Next, open a new Notebook with a PySpark kernel.

4. Follow the Notebook to complete the query.


### Part G - Run the PySpark script using spark-submit command
1. To use the spark-submit command, we need to SSH into the master node by using the following command in the terminal, where **xxx.pem** is the secret access key created in Part A:
    ```
    ssh -i xxx.pem hadoop@ec2-54-144-xxx-xxx.compute-1.amazonaws.com
    ```

2. Use the command `which spark-submit` to find out the location of it.

3. Run the script:

    ```
    /usr/bin/spark-submit --master yarn hdfs:///pyspark-script/emr-pyspark-code.py
    ```

4. We may not be able to see the immediate query result since it will be written inside a flood of runtime info. The complete results, however, is still available in the `results` folder.

### Part H - Create a table and run the query in Hive
1. Also inside the master node, type `hive` to access Hive.

2. Use the `CREATE TABLE` SQL command in `hive-table.txt` to create a table:

    ```sql
    CREATE EXTERNAL TABLE lab1 (
    gender STRING,
    title STRING,
    first STRING,
    last STRING,
    street_number INT,
    street_name STRING,
    city STRING,
    state STRING,
    country STRING,
    postcode INT,
    latitude DOUBLE,
    longitude DOUBLE,
    offset STRING,
    description STRING,
    email STRING,
    dob STRING,
    age INT,
    date1 STRING,
    reg_age INT,
    phone STRING,
    cell STRING,
    id STRING,
    value STRING,
    picture_large STRING,
    picture_medium STRING,
    picture_thumbnail STRING,
    nat STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 's3://das-c01-data-analytics-specialty/Data_Analytics_With_Spark_and_EMR/user-data-acg/'
    tblproperties ("skip.header.line.count"="1");
    ```

3. Use the bottom half of the `hive-table.txt` to run the query:
    ```sql
    SELECT age, gender, COUNT(*) AS total
    FROM lab1
    GROUP BY age, gender
    ORDER BY total DESC
    LIMIT 20;
    ```

4. The result is the same as the one from using Spark.  

    ![hive_query](/images/hive-query.png)

5. Type `exit` to exit Hive.

### Part I - Run the query in spark-sql
1. Type `spark-sql` to enter Spark SQL.

2. Cache the table we've created in Part H into the memory by typing `cache table lab1;`.

3. Run the query as in Part H, and we will get the same results:

    ![use_ssh](/images/use_ssh.png)

### Part J - Copy data from EMR back to S3
1. Create a bucket in S3. Here I name it **emr-lab1-2021**.

2. Add a final step to the EMR cluster:
    * Step type: Custom JAR
    * Name: Copy data to S3
    * JAR location: command-runner.jar
    * Arguments:
        ```
        s3-dist-cp --src=hdfs:///results --dest=s3://emr-lab1-2021/
        ```
    * Action on failure: Continue

3. This will make a copy of the query result to S3, which completes this lab.


