# Models

Data models represented in [Apache Avro](https://avro.apache.org/docs/current/) binary format, generated from Avro schemas.

## Generate Avro Schema from a Java class using Avro Tools

1. Download avro-tools jar from the official repository.
2. Run the induce command passing the classPath as 1st argument and the className as 2nd argument. For example:

java -jar avro-tools-1.8.2.jar induce models-BUILD_VERSION.jar org.gbif.pipelines.io.avro.BasicRecord > basic-record.avsc