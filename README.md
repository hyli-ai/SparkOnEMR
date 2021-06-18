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

![flowchart](/images/flowchart)

### Goal
The goal of this project, is to present Amazon EMR from an operation point of view. This includes the following parts:

* Create and configure an EMR cluster

* Copy data from S3 to EMR using the `s3-dist-cp` command

* Query the data using three different methods:
    1. Run the PySpark script in a **Step** using `spark-submit`
    2. Run the PySpark script using Jupyter Notebook
    3. Create a table and run the query in Hive

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
    Release: emr-5.31.0 <sup>[1](#myfootnote1)</sup>  
    Applications: Hadoop 2.10.1  
                  Hive 2.3.7  
                  Spark 2.4.7  
                  Hue 4.9.0 (optional)  
                  Pig 0.17.0 (optional)  
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

    <a name="myfootnote1"><sup>1</sup></a>  According to [Amazon EMR documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-considerations.html), "For EMR release versions 5.32.0 and later, or 6.2.0 and later, your cluster must also be running the Jupyter Enterprise Gateway application in order to work with EMR Notebooks."

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

    ![hadoop_overview](/images/hadoop_overview.png)

    ![hadoop_files](/images/hadoop_files.png)



### Part D - Copy data from S3 to EMR
1. Add a step  
    ```
    s3-dist-cp --src=s3://das-c01-data-analytics-specialty/Data_Analytics_With_Spark_and_EMR/ --dest=hdfs:///
    ```
    ![copy_data_to_hdfs](/images/copy_data_to_hdfs.png)

    ![step_complete](/images/step_complete.png)

    ![hadoop_files_new](/images/hadoop_files_new.png)

    ![uploaded_files](/images/uploaded_files.png)

    ![python_script](/images/python_script.png)

### Part E - Run the PySpark script in a **Step** using `spark-submit`
1. Add another step  
    ```
    spark-submit hdfs:///pyspark-script/emr-pyspark-code.py
    ```

    ![run_pyspark_step](/images/run_pyspark_step.png)

    ![stdout](/images/stdout.png)

    ![query_result](/images/query_result.png)

### Part F - Run the PySpark script using Jupyter Notebook
1. check using ssh  
    ssh -i xxx.pem hadoop@ec2-54-144-xxx-xxx.compute-1.amazonaws.com

    hdfs dfs -ls /

    /usr/bin/spark-submit --master yarn hdfs:///pyspark-script/emr-pyspark-code.py

    ![use_ssh](/images/use_ssh.png)

### Part G - Create a table and run the query in Hive

    hive
    exit;
    spark-sql
    cache table lab1;

    ![hive_query](/images/hive-query)

    ![step_to_s3](/images/step_to_s3)
    ```
    s3-dist-cp --src=hdfs:///results --dest=s3://<YOUR_BUCKET_NAME>/
    ```

### Part H - Copy data from EMR back to S3