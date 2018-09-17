# Core

Main module contains API like classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc.

## Main API classes:
- [converters](./core/src/main/java/org/gbif/pipelines/core/converters)
    - [ExtendedRecordConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/ExtendedRecordConverter.java)
    - [GbifJsonConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/GbifJsonConverter.java)
    - [JsonConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/JsonConverter.java)
- [interpreters](./core/src/main/java/org/gbif/pipelines/core/interpreters)
    - [BasicInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/BasicInterpreter.java)
    - [LocationInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/LocationInterpreter.java)
    - [MetadataInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/MetadataInterpreter.java)
    - [MultimediaInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/MultimediaInterpreter.java)
    - [TaxonomyInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/TaxonomyInterpreter.java)
    - [TemporalInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/TemporalInterpreter.java)
- [io](./core/src/main/java/org/gbif/pipelines/core/io)
    - [DwcaReader.java](./core/src/main/java/org/gbif/pipelines/core/io/DwcaReader.java)
- [Interpretation.java](./core/src/main/java/org/gbif/pipelines/core/Interpretation.java)
- [RecordType.java](./core/src/main/java/org/gbif/pipelines/core/RecordType.java)