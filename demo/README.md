## Demo

Demonstration pipelines to get started.  Run these in your favourite IDE!

### Convert a DwCA to an Avro file

The DwC2AvroPipeline demonstrates the reading of a DwC-A file converting the records into a new format and saving the result as an avro file.
 
It should be possibile to run it in your IDE as it uses the native runner with the output stored in the `demo/output` directory. 


### Create a Hive table to query an Avro file 
 
CREATE TABLE raw_avro 
ROW FORMAT SERDE 
'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED as INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
TBLPROPERTIES ( 
'avro.schema.literal'='{ 
  "name": "ExtendedRecord", 
  "namespace": "org.gbif.pipelines.io.avro", 
  "doc": "A container for an extended DwC record (core plus extension data for a single record)", 
  "type": "record", 
  "fields": [ 
    { 
      "name": "id", 
      "type": "string", 
      "doc": "The record id" 
    }, 
    { 
      "name": "coreTerms", 
      "doc": "The core record terms", 
      "default": {}, 
      "type": { 
        "type": "map", 
        "values": "string" 
      } 
    }, 
    { 
      "name": "extensions", 
      "doc": "The extensions records", 
      "default": {}, 
      "type": { 
        "type": "map", 
        "values": { 
          "type": "array", 
          "doc": "The extension records", 
          "default": [], 
          "items": { 
            "type": "map", 
            "doc": "The extension record terms", 
            "default": {}, 
            "values": [ 
              "null", 
              "string" 
            ] 
          } 
        } 
      } 
    } 
  ] 
} 
');
