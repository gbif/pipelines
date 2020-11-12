#!/usr/bin/env bash
source set-env.sh
java -Xmx8g -Xmx8g  -XX:+UseG1GC -cp $MIGRATION_JAR au.org.ala.pipelines.spark.MigrateUUIDPipeline \
  --inputPath=$FS_PATH/migration/occ_uuid.csv \
  --targetPath=$FS_PATH/$DATA_PATH \
  --hdfsSiteConfig=$HDFS_CONF
