#!/bin/bash -e

VERSION=$1
UUID=$2
IN=$3
OUT=$4

echo
echo "INFO: Running 2-interpretation.sh. Version: ${VERSION}, UUID: ${UUID}, input: ${IN}, output: ${OUT}"
echo

java -XX:+PerfDisableSharedMem -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+UseCMSInitiatingOccupancyOnly -Xms256M -cp ../../gbif/pipelines/ingest-gbif-java/target/ingest-gbif-java-${VERSION}-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline \
  --runner=SparkRunner \
  --datasetId=${UUID} \
  --attempt=1 \
  --interpretationTypes=ALL \
  --targetPath=${OUT}\
  --inputPath=${IN} \
  --properties=configs/pipelines.yaml \
  --useExtendedRecordId=true \
  --useMetadataWsCalls=false \
  --metaFileName=verbatim-to-interpreted.yml
