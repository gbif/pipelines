#!/bin/bash -e

VERSION=$1
UUID=$2
OUT=$3
ES_HOSTS=$4

cd ../
ES_SCHEMA=$(pwd)/src/main/resources/elasticsearch/es-event-core-schema.json
echo ${ES_SCHEMA}

cd event-to-elasticsearch

echo
echo "INFO: Running 3-indexing.sh. Version: ${VERSION}, UUID: ${UUID}, input/output: ${OUT}"
echo

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
    hdfs://ha-nn/pipelines/jars/examples-pipelines.jar \
    --datasetId=${UUID} \
    --attempt=1 \
    --runner=SparkRunner \
    --inputPath=${OUT} \
    --targetPath=${OUT} \
    --metaFileName=interpreted-to-index.yml \
    --hdfsSiteConfig=/home/crap/config/hdfs-site.xml \
    --coreSiteConfig=/home/crap/config/core-site.xml \
    --esHosts=${ES_HOSTS} \
    --esIndexName=event_${UUID} \
    --esSchemaPath=${ES_SCHEMA} \
    --esAlias=event \
    --indexNumberShards=1 \
    --indexNumberReplicas=0 \
    --experiments=use_deprecated_read \
    --esDocumentId=internalId
