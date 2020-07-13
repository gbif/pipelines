#!/usr/bin/env bash

#PIPELINES_JAR=/efs-mount-point/pipelines.jar
export MIGRATION_JAR="/efs-mount-point/migration.jar"

# HDFS_CONF is just blank when not using HDFS
#export HDFS_CONF=/efs-mount-point/hdfs-site.xml
#export HDFS_CONF=
export HDFS_CONF="/efs-mount-point/hdfs-site.xml"

export FS_PATH="hdfs://aws-spark-quoll-1.ala:9000"

export DATA_PATH="pipelines-data"

export SPARK_MASTER="spark://aws-spark-quoll-1.ala:7077"