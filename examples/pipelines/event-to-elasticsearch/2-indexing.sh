#!/bin/bash -e

VERSION=$1
UUID=$2
HDFS_PATH=$3
ES_HOSTS=$4

echo
echo "INFO: Running 2-indexing.sh. Version: ${VERSION}, UUID: ${UUID}, hdfs root path: ${HDFS_PATH}"
echo

cd ../
JAR_PATH=$(pwd)/target/examples-pipelines-${VERSION}-shaded.jar
echo ${JAR_PATH}
cd event-to-elasticsearch

sudo -u hdfs spark2-submit \
    --queue root.pipelines \
    --conf spark.executor.memoryOverhead=1280 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.yarn.am.waitTime=360s \
    --class org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexPipeline \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 4G \
    --executor-cores 2 \
    --num-executors 2 \
    --driver-memory 1G \
    ${JAR_PATH} \
    --datasetId=${UUID} \
    --attempt=1 \
    --runner=SparkRunner \
    --inputPath=${HDFS_PATH} \
    --targetPath=${HDFS_PATH} \
    --metaFileName=interpreted-to-index.yml \
    --hdfsSiteConfig=/home/crap/config/hdfs-site.xml \
    --coreSiteConfig=/home/crap/config/core-site.xml \
    --esHosts=${ES_HOSTS} \
    --esIndexName=event_${UUID} \
    --esSchemaPath=elasticsearch/es-event-core-schema.json \
    --esAlias=event \
    --indexNumberShards=1 \
    --indexNumberReplicas=0 \
    --experiments=use_deprecated_read \
    --esDocumentId=internalId
