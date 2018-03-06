# Core

The core Avro schemas, models, SerDes and interpretation functions that are the building blocks for pipelines.

Great care should be taken with all dependencies and an attempt to ensure blocks are as reusable as possible.


## Generate Avro Schema from a Java class using Avro Tools

1. Download avro-tools jar from the official repository.
2. Run the induce command passing the classPath as 1st argument and the className as 2nd argument. For example:

java -jar avro-tools-1.8.2.jar induce gbif-api-0.59-SNAPSHOT.jar org.gbif.api.v2.NameUsageMatch2 > taxonSchema.avsc

java -jar avro-tools-1.7.7.jar induce ~/.m2/repository/org/gbif/gbif-api/0.59-SNAPSHOT/gbif-api-0.59-SNAPSHOT.jar:guava-11.0.2.jar org.gbif.api.v2.NameUsageMatch2 > taxonSchema-1-7.avsc