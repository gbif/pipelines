# Base

Base [Apache Beam](https://beam.apache.org/get-started/beam-overview/) transformations and pipelines for ingestion biodiversity data.

## Main API classes:
- [options](./base/src/main/java/org/gbif/pipelines/base/options)
    - [BasePipelineOptions.java](./base/src/main/java/org/gbif/pipelines/base/options/BasePipelineOptions.java)
    - [EsPipelineOptions.java](./base/src/main/java/org/gbif/pipelines/base/options/EsPipelineOptions.java)
    - [IndexingPipelineOptions.java](./base/src/main/java/org/gbif/pipelines/base/options/IndexingPipelineOptions.java)
    - [InterpretationPipelineOptions.java](./base/src/main/java/org/gbif/pipelines/base/options/InterpretationPipelineOptions.java)
    - [PipelinesOptionsFactory.java](./base/src/main/java/org/gbif/pipelines/base/options/PipelinesOptionsFactory.java)
- [pipelines](./base/src/main/java/org/gbif/pipelines/base/pipelines)
    - [DwcaToAvroPipeline.java](./base/src/main/java/org/gbif/pipelines/base/pipelines/DwcaToAvroPipeline.java)
    - [IndexingPipeline.java](./base/src/main/java/org/gbif/pipelines/base/pipelines/IndexingPipeline.java)
    - [IndexingWithCreationPipeline.java](./base/src/main/java/org/gbif/pipelines/base/pipelines/IndexingWithCreationPipeline.java)
    - [InterpretationPipeline.java](./base/src/main/java/org/gbif/pipelines/base/pipelines/InterpretationPipeline.java)
- [transforms](./base/src/main/java/org/gbif/pipelines/base/transforms)
    - [CheckTransforms.java](./base/src/main/java/org/gbif/pipelines/base/transforms/CheckTransforms.java)
    - [MapTransforms.java](./base/src/main/java/org/gbif/pipelines/base/transforms/MapTransforms.java)
    - [ReadTransforms.java](./base/src/main/java/org/gbif/pipelines/base/transforms/ReadTransforms.java)
    - [RecordTransforms.java](./base/src/main/java/org/gbif/pipelines/base/transforms/RecordTransforms.java)
    - [UniqueIdTransform.java](./base/src/main/java/org/gbif/pipelines/base/transforms/UniqueIdTransform.java)
    - [WriteTransforms.java](./base/src/main/java/org/gbif/pipelines/base/transforms/WriteTransforms.java)
- [utils](./base/src/main/java/org/gbif/pipelines/base/utils)
    - [FsUtils.java](./base/src/main/java/org/gbif/pipelines/base/utils/FsUtils.java)
    - [IndexingUtils.java](./base/src/main/java/org/gbif/pipelines/base/utils/IndexingUtils.java)