#!/bin/bash -e

echo
echo "INFO: Start"

UUID=$1
HDFS_PATH=$2
ES_HOSTS=$3

VERSION=$(cat ../pom.xml | grep '<version>' | cut -d'>' -f 2 | cut -d'<' -f 1)

echo "INFO: Use pipelines version: ${VERSION}, UUID: ${UUID}, hdfs root path: ${HDFS_PATH}"

echo "Copying pipelines.yaml to ${HDFS_PATH}/${UUID}/1/"
sudo -u hdfs hdfs dfs -copyFromLocal -f configs/pipelines.yaml ${HDFS_PATH}/${UUID}/1/pipelines.yaml

./1-interpretation.sh ${VERSION} ${UUID} ${HDFS_PATH}

./2-indexing.sh ${VERSION} ${UUID} ${HDFS_PATH} ${ES_HOSTS}

echo "INFO: End"
echo
echo "INFO: Check the ES index name event_example"
echo
