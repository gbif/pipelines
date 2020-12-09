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
--driver-java-options "-Dlog4j.configuration=file:/data/la-pipelines/config/log4j.properties" \
$MIGRATION_JAR \
--occUuidExportPath=$FS_PATH/migration/occ_uuid.csv \
--occFirstLoadedExportPath=$FS_PATH/migration/occ_first_loaded_date.csv \
--targetPath=$FS_PATH/$DATA_PATH \
--hdfsSiteConfig=$HDFS_SITE_CONF \
--coreSiteConfig=$CORE_SITE_CONF
