#!/bin/bash -e

VERSION=$1
UUID=$2
OUT=$3
ES_HOSTS=$4

cd ../../
ES_SCHEMA=$(pwd)/gbif/pipelines/ingest-gbif-beam/src/main/resources/elasticsearch/es-occurrence-schema.json
echo ${ES_SCHEMA}

cd examples/dwca-to-elasticsearch


echo
echo "INFO: Running 3-indexing.sh. Version: ${VERSION}, UUID: ${UUID}, input/output: ${OUT}"
echo

java -XX:+PerfDisableSharedMem -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+UseCMSInitiatingOccupancyOnly -Xms256M -cp ../../gbif/pipelines/ingest-gbif-java/target/ingest-gbif-java-${VERSION}-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline \
  --datasetId=${UUID} \
  --attempt=1 \
  --runner=SparkRunner \
  --metaFileName=occurrence-to-index.yml \
  --inputPath=${OUT} \
  --targetPath=${OUT} \
  --esSchemaPath=${ES_SCHEMA} \
  --indexNumberShards=1 \
  --indexNumberReplicas=0 \
  --esDocumentId=id \
  --esAlias=alias_example \
  --esHosts=${ES_HOSTS} \
  --esIndexName=index_name_example \
