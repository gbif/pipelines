#!/usr/bin/env bash
source set-env.sh
java -Xmx8g -Xmx8g  -XX:+UseG1GC \
  -cp $MIGRATION_JAR \
  au.org.ala.pipelines.spark.MigrateUUIDPipeline \
  --occUuidExportPath=$FS_PATH/migration/occ_uuid.csv \
  --occFirstLoadedExportPath=$FS_PATH/migration/occ_first_loaded.csv \
  --targetPath=$FS_PATH/$DATA_PATH \
  --hdfsSiteConfig=$PWD/hdfs-site.xml




