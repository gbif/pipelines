#!/usr/bin/env bash

source set-env.sh

/data/spark/bin/spark-submit \
--name "Migrate UUIDs" \
--conf spark.default.parallelism=192 \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.spark.MigrateUUIDPipeline \
--master $SPARK_MASTER \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
$MIGRATION_JAR \
--occUuidExportPath=$FS_PATH/migration/occ_uuid.csv \
--occFirstLoadedExportPath=$FS_PATH/migration/occ_first_loaded.csv \
--targetPath=$FS_PATH/$DATA_PATH \
--hdfsSiteConfig=$HDFS_CONF


source set-env.sh
java -Xmx8g -Xmx8g  -XX:+UseG1GC \
  -cp $MIGRATION_JAR \
  au.org.ala.pipelines.spark.MigrateUUIDPipeline \
  --occUuidExportPath=$FS_PATH/migration/occ_uuid.csv \
  --occFirstLoadedExportPath=$FS_PATH/migration/occ_first_loaded.csv \
  --targetPath=$FS_PATH/$DATA_PATH \
  --hdfsSiteConfig=$PWD/hdfs-site.xml