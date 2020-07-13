#!/usr/bin/env bash

source set-env.sh

echo "UUID pipeline - Preserve or add UUIDs."

if [ $# -eq 0 ]
  then
    echo "Please supply a data resource UID"
    exit 1
fi

java -Xmx8g -Xmx8g -XX:+UseG1GC  -cp $PIPELINES_JAR  au.org.ala.pipelines.beam.ALAUUIDMintingPipeline \
 --datasetId=$1\
 --config=../configs/la-pipelines.yaml,../configs/la-pipelines-spark-embedded.yaml,../configs/la-pipelines-local.yaml