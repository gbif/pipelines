#!/bin/bash -e

echo
echo "INFO: Start"

UUID=$1
HDFS_PATH=$2
ES_HOSTS=$3

VERSION=$(cat ../pom.xml | grep '<version>' | cut -d'>' -f 2 | cut -d'<' -f 1)

echo "INFO: Use pipelines version: ${VERSION}, UUID: ${UUID}, hdfs root path: ${HDFS_PATH}"

./2-interpretation.sh ${VERSION} ${UUID} ${OUT}

./3-indexing.sh ${VERSION} ${UUID} ${OUT} ${ES_HOSTS}

echo "INFO: End"
echo
echo "INFO: Check the ES index name event_example"
echo
