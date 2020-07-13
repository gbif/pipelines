#!/usr/bin/env bash

export MIGRATION_JAR="/tmp/migration-1.0-SNAPSHOT-shaded.jar"

# HDFS_CONF is just blank when not using HDFS
#export HDFS_CONF=/efs-mount-point/hdfs-site.xml
#export HDFS_CONF=
export HDFS_CONF="/tmp/hdfs-site.xml"

#export DATA_PATH=hdfs://aws-spark-quoll-1.ala:9000
#export DATA_PATH="/data"
export FS_PATH="hdfs://localhost:8020"

export DATA_PATH="pipelines-data"

