## Demo

Demonstration pipelines to get started.  Run these in your favourite IDE!

### Convert a DwCA to an Avro file

The DwC2AvroPipeline demonstrates the reading of a DwC-A file converting the records into a new format and saving the result as an avro file.
 
It should be possibile to run it in your IDE as it uses the native runner with the output stored in the `demo/output` directory. 


### Exporting to HDFS

Usage examples to run the pipelines DwcaToHdfsPipeline and AvroToHdfsPipeline on HDFS: 
 
``` 
sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.hdfs.AvroToHdfsPipeline --defaultTargetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/exportData* --runner=DirectRunner

sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.hdfs.DwcaToHdfsPipeline --defaultTargetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/dwca.zip --runner=DirectRunner
 
```

To check all the parameters available take a look at the HdfsExporterOptions class. 
Also notice that some parameters have default values, in case no value is provided.

### Demo interpret taxonomic fields

Run demo pipeline:

```
sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.TaxonomyInterpretationPipeline --runner=DirectRunner

sudo -u hdfs spark-submit --conf spark.default.parallelism=24 --conf spark.yarn.executor.memoryOverhead=2048 --class org.gbif.pipelines.demo.TaxonomyInterpretationPipeline --master yarn --executor-memory 24G --executor-cores 2 --num-executors 3 /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar --runner=SparkRunner
```

Create hive tables:

```
USE pipelines; 
 
DROP TABLE IF EXISTS taxon_interpreted;
DROP TABLE IF EXISTS taxon_issues_interpreted;

CREATE EXTERNAL TABLE taxon_interpreted
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
LOCATION 'hdfs://ha-nn/pipelines/avrotest1/taxon' 
TBLPROPERTIES ('avro.schema.url'= 'hdfs://ha-nn/pipelines/avroschemas/taxonRecord.avsc'); 

CREATE EXTERNAL TABLE taxon_issues_interpreted
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
LOCATION 'hdfs://ha-nn/pipelines/avrotest1/taxonIssues' 
TBLPROPERTIES ('avro.schema.url'= 'hdfs://ha-nn/pipelines/avroschemas/issues.avsc'); 
```