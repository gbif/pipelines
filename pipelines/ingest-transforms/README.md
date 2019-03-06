# Ingest-transforms

Base [Apache Beam](https://beam.apache.org/get-started/beam-overview/) transformations for ingestion biodiversity data.

## Main API classes:
- [core](./src/main/java/org/gbif/pipelines/transforms/CheckTransforms.java)
    - [BasicTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/BasicTransform.java)
    - [LocationTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/LocationTransform.java)
    - [MetadataTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/MetadataTransform.java)
    - [TaxonomyTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/TaxonomyTransform.java)
    - [TemporalTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/TemporalTransform.java)
    - [VerbatimTransform.java](./src/main/java/org/gbif/pipelines/transforms/core/VerbatimTransform.java)
- [extension](./src/main/java/org/gbif/pipelines/transforms/CheckTransforms.java)
    - [AmplificationTransform.java](./src/main/java/org/gbif/pipelines/transforms/extension/AmplificationTransform.java)
    - [AudubonTransform.java](./src/main/java/org/gbif/pipelines/transforms/extension/AudubonTransform.java)
    - [ImageTransform.java](./src/main/java/org/gbif/pipelines/transforms/extension/ImageTransform.java)
    - [MeasurementOrFactTransform.java](./src/main/java/org/gbif/pipelines/transforms/extension/MeasurementOrFactTransform.java)
    - [MultimediaTransform.java](./src/main/java/org/gbif/pipelines/transforms/extension/MultimediaTransform.java)
- [specific](./src/main/java/org/gbif/pipelines/transforms/CheckTransforms.java)
    - [AustraliaSpatialTransform.java](./src/main/java/org/gbif/pipelines/transforms/specific/AustraliaSpatialTransform.java)
- [CheckTransforms.java](./src/main/java/org/gbif/pipelines/transforms/CheckTransforms.java)
- [UniqueIdTransform.java](./src/main/java/org/gbif/pipelines/transforms/UniqueIdTransform.java)
