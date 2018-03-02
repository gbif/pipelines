## Demo

Demonstration pipelines to get started.  Run these in your favourite IDE!

### Convert a DwCA to an Avro file

The DwC2AvroPipeline demonstrates the reading of a DwC-A file converting the records into a new format and saving the result as an avro file.
 
It should be possibile to run it in your IDE as it uses the native runner with the output stored in the `demo/output` directory. 


### Exporting to HDFS

Usage examples to run the pipelines DwcaToHdfsPipeline and AvroToHdfsPipeline on HDFS: 
 
``` 
sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.AvroToHdfsPipeline --defaultTargetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/exportData* --runner=DirectRunner

sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.DwcaToHdfsPipeline --defaultTargetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/dwca.zip --runner=DirectRunner
 
```

To check all the parameters available take a look at the HdfsExporterOptions class. 
Also notice that some parameters have default values, in case no value is provided.

### Demo Data Ingestion Pipeline (DwCA2InterpretedRecordsPipeline)
This Pipeline can be used to run and produce interpreted records from the raw dwc archive.
#### Using direct runner (running on local mac)
##### Write to local filesystem
```mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip" -Pdirect-runner``` 
##### Write to HDFS
```mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip --HDFSConfigurationDirectory=/Users/clf358/Downloads/hadoop-conf/ --defaultTargetDirectory=hdfs://ha-nn/user/hive/warehouse/gbif-data/abc123/" -Pdirect-runner```
#### Running on spark cluster
##### Write to HDFS
```spark-submit --conf spark.default.parallelism=24 --conf spark.yarn.executor.memoryOverhead=2048 --class org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline --master yarn --executor-memory 24G --executor-cores 2 --num-executors 3 --files /home/rpathak/dwca.zip /home/rpathak/demo-1.0-SNAPSHOT-shaded.jar --runner=SparkRunner --datasetId=abc123 --inputFile=dwca.zip --HDFSConfigurationDirectory=/home/rpathak/conf/ --defaultTargetDirectory=hdfs://ha-nn/user/hive/warehouse/gbif-data/abc123/```
