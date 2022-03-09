#!/bin/bash -e

VERSION=$1
UUID=$2
HDFS_PATH=$3

VERBATIM_PATH=${HDFS_PATH}/${UUID}/1/verbatim.avro

echo
echo "INFO: Running 2-interpretation.sh. Version: ${VERSION}, UUID: ${UUID}, hdfs root path: ${HDFS_PATH}"
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
    --targetPath=${HDFS_PATH} \
    --inputPath=${VERBATIM_PATH} \
    --runner=SparkRunner \
    --metaFileName=verbatim-to-interpreted.yml \
    --avroCompressionType=snappy \
    --avroSyncInterval=2097152 \
    --hdfsSiteConfig=/home/crap/config/hdfs-site.xml \
    --coreSiteConfig=/home/crap/config/core-site.xml \
    --experiments=use_deprecated_read
