## Demo

Demonstration pipelines to get started.  Run these in your favourite IDE!

### Convert a DwCA to an Avro file

The DwC2AvroPipeline demonstrates the reading of a DwC-A file converting the records into a new format and saving the result as an avro file.
 
It should be possibile to run it in your IDE as it uses the native runner with the output stored in the `demo/output` directory. 


### Exporting to HDFS

Usage examples to run the pipelines DwcaToHdfsPipeline and AvroToHdfsPipeline on HDFS: 
 
``` 
sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.hdfs.AvroToHdfsPipeline --targetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/exportData* --runner=DirectRunner

sudo -u hdfs hadoop jar /home/mlopez/demo-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.demo.hdfs.DwcaToHdfsPipeline --targetDirectory=hdfs://ha-nn/pipelines --datasetId=exp1 --inputFile=/home/mlopez/tests/dwca.zip --runner=DirectRunner
 
```

To check all the parameters available take a look at the HdfsExporterOptions class. 
Also notice that some parameters have default values, in case no value is provided.