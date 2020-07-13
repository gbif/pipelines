#!/usr/bin/env bash

source set-env.sh

if [ $# -eq 0 ]
  then
    echo "Please supply a data resource UID"
    exit 1
fi

echo $(date)
SECONDS=0

/data/spark/bin/spark-submit \
--name "uuid-minting $1" \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 1G \
--class au.org.ala.pipelines.beam.ALAUUIDMintingPipeline \
--master $SPARK_MASTER \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
$PIPELINES_JAR \
--datasetId=$1 \
--config=../configs/la-pipelines.yaml,../configs/la-pipelines-spark-cluster.yaml,../configs/la-pipelines-local.yaml

echo $(date)
duration=$SECONDS
echo "Adding uuids to $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."