# GBIF Pipelines

Status: For demonstration only.
 
Prerequisites: A running Elastic Search 5.[4+] cluster (see the [dev-notes](dev-notes.md)).

To build:
```
mvn package
```

Note: a lot of hard coded paths etc in the software.

Pipelines:
 - One pipeline takes a DwC-A file from HDFS and converts it into an Avro file.
 - The second pipleline takes the Avro file and indexes it in Elastic Search. 

To run:
```
sudo -u hdfs hdfs dfs -put ./dwca-lep5.zip /tmp/dwca-lep5.zip

sudo -u hdfs spark-submit --class org.gbif.pipelines.indexing.DwC2AvroPipeline --master yarn --executor-memory 18G --executor-cores 3 --num-executors 1 /
/trobertson/gbif-pipelines-1.0-SNAPSHOT-shaded.jar --runner=SparkRunner


sudo -u hdfs spark-submit --class org.gbif.pipelines.indexing.InterpretDwCAvroPipeline --master yarn --executor-memory 18G --executor-cores 3 --num-executors 1 gbif-pipelines-1.0-SNAPSHOT-shaded.jar --runner=SparkRunner

sudo -u hdfs spark-submit --conf spark.default.parallelism=24 --class org.gbif.pipelines.indexing.Avro2ElasticSearchPipeline --master yarn --executor-memory 8G --executor-cores 8 --num-executors 3 /home/trobertson/gbif-pipelines-1.0-SNAPSHOT-shaded.jar --runner=SparkRunner
```
