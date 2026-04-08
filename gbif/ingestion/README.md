# Ingestion Pipelines

Module uses [Apache Spark](https://spark.apache.org/) to define and execute data processing pipelines

## Module structure:
- [**spark-jobs**](./spark-jobs) - GBIF spark pipelines for ingestion of biodiversity data
- [**spark-coordinator**](./spark-coordinator) - Coordinator for GBIF pipelines, responsible for scheduling and monitoring of pipelines in spark-jobs
- [**healthcheck**](./healthcheck) - The k8s healthcheck for pipelines CLIs
