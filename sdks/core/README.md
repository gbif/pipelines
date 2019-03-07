# Core

Main module contains API like classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc.

## Main API classes:
- [converters](./src/main/java/org/gbif/pipelines/core/converters)
    - [ExtendedRecordConverter.java](./src/main/java/org/gbif/pipelines/core/converters/ExtendedRecordConverter.java)
    - [GbifJsonConverter.java](./src/main/java/org/gbif/pipelines/core/converters/GbifJsonConverter.java)
    - [JsonConverter.java](./src/main/java/org/gbif/pipelines/core/converters/JsonConverter.java)
    - [MultimediaConverter.java](./src/main/java/org/gbif/pipelines/core/converters/MultimediaConverter.java)
- [interpreters](./src/main/java/org/gbif/pipelines/core/interpreters)
    - [core](./src/main/java/org/gbif/pipelines/core/interpreters/core)
        - [BasicInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/BasicInterpreter.java)
        - [ExtendedInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/ExtendedInterpreter.java)
        - [LocationInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/LocationInterpreter.java)
        - [MetadataInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/MetadataInterpreter.java)
        - [MultimediaInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/MultimediaInterpreter.java)
        - [TaxonomyInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/TaxonomyInterpreter.java)
        - [TemporalInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/core/TemporalInterpreter.java)
    - [extension](./src/main/java/org/gbif/pipelines/core/interpreters/extension)
        - [AmplificationInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/extension/AmplificationInterpreter.java)
        - [AudubonInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/extension/AudubonInterpreter.java)
        - [ImageInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/extension/ImageInterpreter.java)
        - [MeasurementOrFactInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/extension/MeasurementOrFactInterpreter.java)
        - [MultimediaInterpreter.java](./src/main/java/org/gbif/pipelines/core/interpreters/extension/MultimediaInterpreter.java)
- [io](./src/main/java/org/gbif/pipelines/core/io)
    - [DwcaReader.java](./src/main/java/org/gbif/pipelines/core/io/DwcaReader.java)
- [ExtensionInterpretation.java](./src/main/java/org/gbif/pipelines/core/ExtensionInterpretation.java)
- [Interpretation.java](./src/main/java/org/gbif/pipelines/core/Interpretation.java)
