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
