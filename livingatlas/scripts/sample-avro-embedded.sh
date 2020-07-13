#!/usr/bin/env bash
source set-env.sh

echo "Add sampling to dataset"

if [ $# -eq 0 ]
  then
    echo "Please supply a data resource UID"
    exit 1
fi

echo $(date)
SECONDS=0

java -Xmx8g -Xmx8g -XX:+UseG1GC  -cp $PIPELINES_JAR au.org.ala.pipelines.beam.ALASamplingToAvroPipeline \
 --datasetId=$1 \
 --config=../configs/la-pipelines.yaml,../configs/la-pipelines-spark-embedded.yaml,../configs/la-pipelines-local.yaml

echo $(date)
duration=$SECONDS
echo "[SAMPLE-AVRO] Adding sampling to $1 took $(($duration / 60)) minutes and $(($duration % 60)) seconds."
