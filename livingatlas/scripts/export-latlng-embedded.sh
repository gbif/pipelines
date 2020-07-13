#!/usr/bin/env bash
source set-env.sh

if [ $# -eq 0 ]
  then
    echo "Please supply a data resource UID"
    exit 1
fi

java -cp $PIPELINES_JAR au.org.ala.pipelines.beam.ALAInterpretedToLatLongCSVPipeline \
 --datasetId=$1 \
 --config=../configs/la-pipelines.yaml,../configs/la-pipelines-local.yaml