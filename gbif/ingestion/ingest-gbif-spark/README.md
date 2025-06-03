## Spark connect example

This example illustrates how to use the pipelines models and interpretation code, using spark connect or submit
and running some interpretation.

Download spark from [here](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3-connect.tgz).

Start it (not sure the packages are needed or not)
``` 
export SPARK_LOCAL_IP="127.0.0.1"
export SPARK_DAEMON_MEMORY="4G"
cd spark-4.0.0-bin-hadoop3-connect
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.0.0 com.esotericsoftware:kryo:5.6.1
```

Build the project in mvn, and run the `Interpretation.java` making sure provided dependencies are on the CP and
that the `conf-local.yaml` references a real verbatim avro file which can be pulled from HDFS (or ask Tim).