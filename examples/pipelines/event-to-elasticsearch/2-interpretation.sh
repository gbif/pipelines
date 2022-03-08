#!/bin/bash -e

VERSION=$1
UUID=$2
IN=$3
OUT=$4

echo
echo "INFO: Running 2-interpretation.sh. Version: ${VERSION}, UUID: ${UUID}, input: ${IN}, output: ${OUT}"
echo

sudo -u hdfs spark2-submit \
    --queue root.pipelines \
    --conf spark.executor.memoryOverhead=1280 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.yarn.am.waitTime=360s \
    --class org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 4G \
    --executor-cores 2 \
    --num-executors 3 \
    --driver-memory 1G \
    hdfs://ha-nn/pipelines/jars/examples-pipelines.jar \
    --datasetId=${UUID} \
    --attempt=1 \
    --targetPath=${OUT} \
    --inputPath=${IN} \
    --runner=SparkRunner \
    --metaFileName=verbatim-to-interpreted.yml \
    --avroCompressionType=snappy \
    --avroSyncInterval=2097152 \
    --hdfsSiteConfig=/home/crap/config/hdfs-site.xml \
    --coreSiteConfig=/home/crap/config/core-site.xml \
    --experiments=use_deprecated_read
