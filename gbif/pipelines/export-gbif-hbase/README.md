# Export GBIF HBase

This is a interim pipeline to export the verbatim data from the GBIF HBase tables and save as `ExtendedRecord` avro files.

To run this in production using an HBase snapshot (e.g. prod_g_occurrence_snapshot):
```
nohup sudo -u hdfs spark2-submit \
  --class org.gbif.pipelines.hbase.beam.ExportHBaseSnapshot \
  --master yarn \
  --executor-memory 64G \
  --driver-memory 4G \
  --conf spark.yarn.executor.memoryOverhead=8G \
  --conf spark.executor.instances=50 \
  --conf spark.executor.cores=1 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.default.parallelism=500 \
  --conf spark.network.timeout=360s \
  --conf spark.executor.heartbeatInterval=120s \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
  export-gbif-hbase-2.2.1-SNAPSHOT-shaded.jar \
  --runner=SparkRunner \
  --hbaseZk=c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org \
  --exportPath=hdfs://ha-nn/pipelines/export-20190707/ \
  --table=prod_g_occurrence_snapshot &
```

If an attempt fails then `.temp-beam-*` will remain in the HDFS folder and can be removed with:
```
sudo -u hdfs hdfs dfs -rm -r /pipelines/export-20190707/.temp*
```

This configuration runs in around 3.5hrs in production.
