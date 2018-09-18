# Base

Base [Apache Beam](https://beam.apache.org/get-started/beam-overview/) transformations and pipelines for ingestion biodiversity data.

## Main API classes:
- [options](./src/main/java/org/gbif/pipelines/ingest/options)
    - [BasePipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/BasePipelineOptions.java)
    - [DwcaPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/DwcaPipelineOptions.java)
    - [EsPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/EsPipelineOptions.java)
    - [IndexingPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/IndexingPipelineOptions.java)
    - [InterpretationPipelineOptions.java](./src/main/java/org/gbif/pipelines/ingest/options/InterpretationPipelineOptions.java)
    - [PipelinesOptionsFactory.java](./src/main/java/org/gbif/pipelines/ingest/options/PipelinesOptionsFactory.java)
- [pipelines](./src/main/java/org/gbif/pipelines/ingest/pipelines)
    - [DwcaToAvroPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/DwcaToAvroPipeline.java)
    - [IndexingPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/IndexingPipeline.java)
    - [IndexingWithCreationPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/IndexingWithCreationPipeline.java)
    - [InterpretationPipeline.java](./src/main/java/org/gbif/pipelines/ingest/pipelines/InterpretationPipeline.java)
- [utils](./src/main/java/org/gbif/pipelines/ingest/utils)
    - [FsUtils.java](./src/main/java/org/gbif/pipelines/ingest/utils/FsUtils.java)
    - [IndexingUtils.java](./src/main/java/org/gbif/pipelines/ingest/utils/IndexingUtils.java)