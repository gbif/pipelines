# Ingest-GBIF

Base [Apache Beam](https://beam.apache.org/get-started/beam-overview/) pipelines for ingestion biodiversity data.

## Main API classes:
- [options](./src/main/java/org/gbif/pipelines/ingest/options)
    - [PipelinesOptionsFactory.java](./src/main/java/org/gbif/pipelines/ingest/options/PipelinesOptionsFactory.java)
    - [BasePipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/BasePipelineOptions.java)
    - [EsPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/EsPipelineOptions.java)
    - [EsIndexingPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/EsIndexingPipelineOptions.java)
    - [InterpretationPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/InterpretationPipelineOptions.java)
    - [DwcaPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/DwcaPipelineOptions.java)
- [pipelines](./src/main/java/org/gbif/pipelines/ingest/pipelines)
    - [DwcaToAvroPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/DwcaToAvroPipeline.java)
    - [DwcaToIngestPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/DwcaToIngestPipeline.java)
    - [DwcaToEsIndexPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/DwcaToEsIndexPipeline.java)
    - [AvroToIngestPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/AvroToIngestPipeline.java)
    - [IngestToEsIndexPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/IngestToEsIndexPipeline.java)
    - [IngestToEsIndexExtendedPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/IngestToEsIndexExtendedPipeline.java)
- [utils](./src/main/java/org/gbif/pipelines/ingest/utils)
    - [FsUtils.java](./src/main/java/org/gbif/pipelines/ingest/utils/FsUtils.java)
    - [EsIndexUtils.java](./src/main/java/org/gbif/pipelines/ingest/utils/EsIndexUtils.java)