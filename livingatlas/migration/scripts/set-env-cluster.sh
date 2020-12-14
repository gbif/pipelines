#!/usr/bin/env bash
export MIGRATION_JAR="/usr/share/la-pipelines/migration.jar"

# HDFS_CONF is just blank when not using HDFS
export HDFS_SITE_CONF="/data/hadoop/etc/hadoop/hdfs-site.xml"

export CORE_SITE_CONF="/data/hadoop/etc/hadoop/core-site.xml"

export FS_PATH="hdfs://aws-spark-quoll-1b.ala:9000"

export DATA_PATH="pipelines-data"

export SPARK_MASTER="spark://aws-spark-quoll-1b.ala:7077"