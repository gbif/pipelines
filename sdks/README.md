# SDKs

Main package contains common classes, such as data models, data interpretations, parsers, web services clients etc.

## Module structure:
- [**core**](./core) - Main API classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc
- [**models**](./models) - Data models represented in [Avro](https://avro.apache.org/docs/current/) binary format, generated from Avro schemas
- [**parsers**](./parsers) - Data parsers and converters, mainly for internal usage inside of interpretations
- [**keygen**](./keygen) - The library to generate GBIF identifier, to support backward compatibility, the codebase (with minimum changes) was copied from occurrence/occurrence-persistence project
