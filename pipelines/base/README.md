# Base

Base [Apache Beam](https://beam.apache.org/get-started/beam-overview/) transformations and pipelines for ingestion biodiversity data.

## Main API classes:
- [options](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options)
    - [BasePipelineOptions.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options/BasePipelineOptions.java)
    - [EsPipelineOptions.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options/EsPipelineOptions.java)
    - [IndexingPipelineOptions.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options/IndexingPipelineOptions.java)
    - [InterpretationPipelineOptions.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options/InterpretationPipelineOptions.java)
    - [PipelinesOptionsFactory.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/options/PipelinesOptionsFactory.java)
- [pipelines](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/pipelines)
    - [DwcaToAvroPipeline.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/pipelines/DwcaToAvroPipeline.java)
    - [IndexingPipeline.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/pipelines/IndexingPipeline.java)
    - [IndexingWithCreationPipeline.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/pipelines/IndexingWithCreationPipeline.java)
    - [InterpretationPipeline.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/pipelines/InterpretationPipeline.java)
- [transforms](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms)
    - [CheckTransforms.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/CheckTransforms.java)
    - [MapTransforms.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/MapTransforms.java)
    - [ReadTransforms.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/ReadTransforms.java)
    - [RecordTransforms.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/RecordTransforms.java)
    - [UniqueIdTransform.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/UniqueIdTransform.java)
    - [WriteTransforms.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/transforms/WriteTransforms.java)
- [utils](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/utils)
    - [FsUtils.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/utils/FsUtils.java)
    - [IndexingUtils.java](https://github.com/gbif/artery/tree/master/pipelines/base/src/main/java/org/gbif/pipelines/base/utils/IndexingUtils.java)