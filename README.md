# WK8-Data-Pipelines-with-Airflow

# Background Information
Our telecommunications company, MTN Rwanda, has a vast customer base, and we generate a large amount of data daily. We must efficiently process and store this data to make informed business decisions. Therefore, we plan to develop a data pipeline to extract, transform, and load data from three CSV files and store it in a Postgres database. We require a skilled data engineer who can use the Airflow tool to develop the pipeline to achieve this.

## Problem Statement
The main challenge is that the data generated is in a raw format, and we need to process it efficiently to make it usable for analysis. This requires us to develop a data pipeline that can extract, transform and load the data from multiple CSV files into a single database, which can be used for further analysis.

## Guidelines
The data pipeline should be developed using Airflow, an open-source tool for creating and managing data pipelines. 
The following steps should be followed to develop the data pipeline:
● The data engineer should start by creating a DAG (Directed Acyclic Graph) that defines the workflow of the data pipeline.

● The DAG should include tasks that extract data from the three CSV files.

● After extraction, the data should be transformed using Python libraries to match the required format.

● Finally, the transformed data should be loaded into a Postgres database.

● The data pipeline should be scheduled to run at a specific time daily using the Airflow scheduler.

● We can use the shared file (mtnrwanda-dag.py) as a starting point.

## Sample CSV Files
The following are sample CSV files that will be used in the data pipeline:

● customer_data.csv

● order_data.csv

● payment_data.csv

All files for this project can be downloaded from here (link).

## Deliverables
We will be expected to deliver a GitHub repository with the following:

● Airflow DAG file for the data pipeline.

● Documentation of the pipeline.

○ Highlight at least 3 best practices used during the implementation. 

○ Recommendations for deployment and running the pipeline in a cloud-based provider. 

# Documentation of the Pipeline:

The data pipeline consists of three tasks: extract, transform, and load. The extract task extracts data from three CSV files stored in the local file system. The transform task cleans and aggregates the extracted data to produce a new DataFrame that contains the customer lifetime value (CLV) for each customer. The load task loads the transformed data into a PostgreSQL database table called "customer_ltv."

The pipeline is designed to run on a daily basis using Airflow, a popular open-source platform for creating, scheduling, and monitoring data pipelines. The pipeline is written in Python and uses several libraries, including Pandas, Psycopg2, and Airflow.

## Best Practices:

Modularity: The pipeline is modular, which makes it easier to maintain and update. Each task has a well-defined input and output, making it easy to test and debug individual tasks independently.

Error Handling: The pipeline includes error handling for each task, which ensures that the pipeline continues to run even if a task fails. Each task logs any errors or exceptions that occur, making it easier to troubleshoot and debug.

Documentation: The pipeline includes detailed documentation, including comments in the code and this document. The documentation makes it easier for other developers to understand how the pipeline works and how to modify or extend it.

## Recommendations for Deployment:

-The pipeline can be deployed to a cloud-based provider, such as Amazon Web Services (AWS), Microsoft Azure, or Google Cloud Platform (GCP). Here are some recommendations for deploying and running the pipeline in a cloud-based environment:

-Use a managed PostgreSQL database service: Instead of running a PostgreSQL database on a virtual machine, use a managed PostgreSQL database service provided by the cloud provider. This will simplify database management and reduce operational overhead.

-Use object storage: Store the input CSV files in object storage (e.g., AWS S3, Azure Blob Storage, or Google Cloud Storage) instead of storing them in the local file system. This will make it easier to scale the pipeline and reduce the risk of data loss.

-Use a containerized approach: Use Docker to containerize the pipeline and deploy it to a container orchestration platform, such as Kubernetes. This will make it easier to manage and scale the pipeline and reduce the risk of environment-related issues.

-Use a monitoring and alerting service: Use a monitoring and alerting service, such as AWS CloudWatch, Azure Monitor, or GCP Stackdriver, to monitor the pipeline and receive alerts if any issues occur. This will help ensure that the pipeline is running smoothly and reduce the risk of data loss or downtime.
