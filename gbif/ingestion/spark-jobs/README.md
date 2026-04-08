## Spark connect example

This example illustrates how to use the new pipelines models and interpretation code, using spark connect or submit
and running some interpretation.

### Install spark locally

Download [the prebuilt for Hadoop 3.3 and later Spark](https://spark.apache.org/downloads.html) and 
start it (might need adjusted for versions):

```
cd spark-3.5.6-bin-hadoop3
export SPARK_LOCAL_IP="127.0.0.1"
export SPARK_DAEMON_MEMORY="4G"
 ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.6,org.apache.spark:spark-avro_2.12:3.5.6                         
```

Check it runs on http://localhost:4040/

Build the project in mvn, and run the `Interpretation.java` making sure provided dependencies are on the CP and
that the `conf-local.yaml` references the correct Jar and a real verbatim avro file which can be pulled from HDFS 
(or ask Tim).


create 'dave_occurrence_lookup', {NAME => 'o', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}, {SPLITS => ['1','2','3','4','5','6','7','8','9']}

create 'dave_occurrence_counter', {NAME => 'o', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', COMPRESSION => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', BLOCKCACHE =>
'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'} 