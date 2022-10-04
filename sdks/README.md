# SDKs

Main package contains common classes, such as data models, data interpretations, parsers, web services clients etc.

## Module structure:
- [**beam-common**](./beam-common) - Classes and API for using with Apache Beam
- [**beam-transforms**](./beam-transforms) - Transformations for ingestion of biodiversity data
- [**core**](./core) - Main API classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader etc.
- [**models**](./models) - Data models represented in Avro binary format, generated from [Avro](https://avro.apache.org/docs/current/) schemas
- [**variables**](./variables) - Only static string variables
- [**archives-converters**](./archives-converters) - Converters from [DwCA/DWC 1.0/DWC](https://www.tdwg.org/standards/dwc/) 1.4/ABCD 1.2/ABCD 2.06 to *.avro format
- [**elasticsearch-tools**](./elasticsearch-tools) - Tool for creating/deleting/swapping [Elasticsearch](https://www.elastic.co/products/elasticsearch) indexes
- [**pipelines-maven-plugin**](./pipelines-maven-plugin) - Maven plugin adds new annotations and interface to avro generated classes
