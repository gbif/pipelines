# DwCASchemaGenerator
The project generates avro schema for all the DwCATerm Categories. The terms and categories are parsed from the link
http://tdwg.github.io/dwc/terms/index.htm, and generates corresponding schema from that. 

The project currently generates an avro schema. But this project can be used to generate any schema type by implementing DwCASchemaBuilder.

# Usage
Generating DwCA based avro schema to a particular folder.

mvn exec:java -D"exec.mainClass"="org.gbif.DwCAAvroSchemaGenerator" -Dexec.args="/path/to/schema/"
