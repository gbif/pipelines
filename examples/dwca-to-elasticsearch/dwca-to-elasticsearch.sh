#!/bin/bash -e

echo
echo "INFO: Start"

IN=$1
OUT=$2
ES_HOSTS=$3

UUID=$(uuidgen)
VERSION=$(cat ../pom.xml | grep '<version>' | cut -d'>' -f 2 | cut -d'<' -f 1)

echo "INFO: Use pipelines version: ${VERSION}, UUID: ${UUID}, input: ${IN}, output: ${OUT}"

VERBATIM_OUTPUT=${OUT}/${UUID}/1/verbatim.avro
./1-dwca-to-avro.sh ${VERSION} ${IN} ${VERBATIM_OUTPUT}

./2-interpretation.sh ${VERSION} ${UUID} ${VERBATIM_OUTPUT} ${OUT}

./3-indexing.sh ${VERSION} ${UUID} ${OUT} ${ES_HOSTS}

echo "INFO: End"
echo
echo "INFO: Check the ES index name index_name_example"
echo
