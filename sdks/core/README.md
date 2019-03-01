# Core

Main module contains API like classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc.

## Main API classes:
- [converters](./core/src/main/java/org/gbif/pipelines/core/converters)
    - [ExtendedRecordConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/ExtendedRecordConverter.java)
    - [GbifJsonConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/GbifJsonConverter.java)
    - [JsonConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/JsonConverter.java)
    - [MultimediaConverter.java](./core/src/main/java/org/gbif/pipelines/core/converters/MultimediaConverter.java)
- [interpreters](./core/src/main/java/org/gbif/pipelines/core/interpreters)
    - [core](./core/src/main/java/org/gbif/pipelines/core/interpreters/core)
        - [BasicInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/BasicInterpreter.java)
        - [ExtendedInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/ExtendedInterpreter.java)
        - [LocationInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/LocationInterpreter.java)
        - [MetadataInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/MetadataInterpreter.java)
        - [MultimediaInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/MultimediaInterpreter.java)
        - [TaxonomyInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/TaxonomyInterpreter.java)
        - [TemporalInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/core/TemporalInterpreter.java)
    - [extension](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension)
        - [AmplificationInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension/AmplificationInterpreter.java)
        - [AudubonInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension/AudubonInterpreter.java)
        - [ImageInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension/ImageInterpreter.java)
        - [MeasurementOrFactInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension/MeasurementOrFactInterpreter.java)
        - [MultimediaInterpreter.java](./core/src/main/java/org/gbif/pipelines/core/interpreters/extension/MultimediaInterpreter.java)
- [io](./core/src/main/java/org/gbif/pipelines/core/io)
    - [DwcaReader.java](./core/src/main/java/org/gbif/pipelines/core/io/DwcaReader.java)
- [ExtensionInterpretation.java](./core/src/main/java/org/gbif/pipelines/core/ExtensionInterpretation.java)
- [Interpretation.java](./core/src/main/java/org/gbif/pipelines/core/Interpretation.java)
