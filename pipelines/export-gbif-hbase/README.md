# Export GBIF HBase

This is a interim pipeline to export the verbatim data from the GBIF HBase tables and save as `ExtendedRecord` avro files.

To run this:
```
java -jar target/export-gbif-hbase-2.1.4-SNAPSHOT-shaded.jar \
 --runner=SparkRunner \
 --hbaseZk=c3zk1.gbif-dev.org,c3zk2.gbif-dev.org,c3zk3.gbif-dev.org \
 --exportPath=/tmp/delme \
 --table=dev_occurrence
```