if [ "${AreYouSure}" = "yes" ]
then
	echo 'Removing old migration data'
	sudo -u spark rm -f /data/migration/*

  echo 'Exporting UUIDs COPY CQLSH exports from Cassandra cluster'
  sudo -u spark ssh aws-cass-cluster-1b.ala '/data/uuid-exports/uuid-export.sh'

	echo 'Retrieving COPY CQLSH exports from Cassandra cluster'
	sudo -u spark scp aws-cass-cluster-1b.ala:/data/uuid-exports/*.csv.gz /data/migration
	cd /data/migration
	sudo -u spark gzip -d *

	echo 'Setting up HDFS directories'
	sudo -u hdfs /data/hadoop/bin/hdfs dfs -mkdir -p /migration
	sudo -u hdfs /data/hadoop/bin/hdfs dfs -chown spark:spark /migration
	sudo -u spark /data/hadoop/bin/hdfs dfs -copyFromLocal -f /data/migration/*.csv /migration/
	sudo -u hdfs /data/hadoop/bin/hdfs dfs -mkdir -p /pipelines-data
	sudo -u hdfs /data/hadoop/bin/hdfs dfs -chown spark:spark /pipelines-data

	echo 'Delete existing identifiers'
  sudo -u hdfs /data/hadoop/bin/hdfs dfs -rm -r /pipelines-data/*/1/identifiers

	# run spark job
	export MIGRATION_JAR="/usr/share/la-pipelines/migration.jar"
	export HDFS_SITE_CONF="/data/hadoop/etc/hadoop/hdfs-site.xml"
	export CORE_SITE_CONF="/data/hadoop/etc/hadoop/core-site.xml"
	export FS_PATH="hdfs://aws-spark-quoll-1b.ala:9000"
	export DATA_PATH="pipelines-data"
	export SPARK_MASTER="spark://aws-spark-quoll-1b.ala:7077"

  echo 'Running spark job to generate UUID AVRO'
	sudo -u spark /data/spark/bin/spark-submit \
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

  echo 'Cleanup temp HDFS directories'
	sudo -u hdfs /data/hadoop/bin/hdfs dfs -rm -r -f /migration
  echo 'Finished'

else
    exit 1
fi