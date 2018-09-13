# SDKs

Main package contains common classes, such as data models, data format iterpretations, parsers, web services clients ant etc.

## Module structure:
- [**core**](https://github.com/gbif/artery/tree/master/sdks/core) - Main API classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc
- [**models**](https://github.com/gbif/artery/tree/master/sdks/models) - Data models represented in [Avro](https://avro.apache.org/docs/current/) binary format, generated from Avro schemas
- [**parsers**](https://github.com/gbif/artery/tree/master/sdks/parsers) - Data parsers and converters, mainly for internal usage inside of interpretations