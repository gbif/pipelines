# Pipelines

Module uses [Apache Beam](https://beam.apache.org/get-started/beam-overview/) as an unified programming model to define and execute data processing pipelines

## Module structure:
- [**export-gbif-hbase**](./export-gbif-hbase) - The pipeline to export the verbatim data from the GBIF HBase tables and save as `ExtendedRecord` avro files
- [**ingest-gbif-beam**](./ingest-gbif-beam) - Main GBIF pipelines for ingestion of biodiversity data
- [**ingest-gbif-fragmenter**](./ingest-gbif-fragmenter) - Writes raw archive's data to HBase store
- [**ingest-gbif-java**](./ingest-gbif-java) - Main GBIF pipelines for ingestion of biodiversity data, Java version
