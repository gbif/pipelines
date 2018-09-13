# Core

Main module contains API like classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc.

## Main API classes:
- [converters](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/converters)
    - [ExtendedRecordConverter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/converters/ExtendedRecordConverter.java)
    - [GbifJsonConverter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/converters/GbifJsonConverter.java)
    - [JsonConverter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/converters/JsonConverter.java)
- [interpreters](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/interpreters)
    - [BasicInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/BasicInterpreter.java)
    - [LocationInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/LocationInterpreter.java)
    - [MetadataInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/MetadataInterpreter.java)
    - [MultimediaInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/MultimediaInterpreter.java)
    - [TaxonomyInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/TaxonomyInterpreter.java)
    - [TemporalInterpreter.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/interpreters/TemporalInterpreter.java)
- [io](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/io)
    - [DwcaReader.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/io/DwcaReader.java)
- [Interpretation.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/Interpretation.java)
- [RecordType.java](https://github.com/gbif/artery/tree/master/sdks/core/src/main/java/org/gbif/pipelines/core/RecordType.java)