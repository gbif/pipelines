# Pipelines

Module uses [Apache Beam](https://beam.apache.org/get-started/beam-overview/) as an unified programming model to define and execute data processing pipelines

## Module structure:
- [**beam-common**](./beam-common) - Classes and API for using with Apache Beam
- [**common**](./common) - Only static string variables
- [**export-gbif-hbase**](export-gbif-hbase) - The pipeline to export the verbatim data from the GBIF HBase tables and save as `ExtendedRecord` avro files
- [**ingest-gbif**](ingest-gbif) - Main GBIF pipelines for ingestion biodiversity data
- [**ingest-gbif-standalone**](./ingest-gbif-standalone) - Independent GBIF pipelines for ingestion biodiversity data
- [**ingest-hdfs-table**](./ingest-hdfs-table) - Pipeline classes for conversion from interpreted formats into one common for HDFS view creation
- [**ingest-transforms**](./ingest-transforms) - Transformations and pipelines for ingestion biodiversity data
