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
--name "SOLR indexing for $1" \
--conf spark.default.parallelism=192 \
--num-executors 24 \
--executor-cores 8 \
--executor-memory 7G \
--driver-memory 4G \
--class au.org.ala.pipelines.beam.ALAInterpretedToSolrIndexPipeline  \
--master $SPARK_MASTER \
--driver-java-options "-Dlog4j.configuration=file:/efs-mount-point/log4j.properties" \
$PIPELINES_JAR \
--datasetId=$1 \
--config=../configs/la-pipelines.yaml,../configs/la-pipelines-spark-cluster.yaml,../configs/la-pipelines-local.yaml

echo $(date)
duration=$SECONDS
echo "[INDEX][SPARK_CLUSTER] Indexing of $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."