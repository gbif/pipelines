#!/bin/bash -e

echo
echo "INFO: Start"

IN=$1
OUT=$2
ES_HOSTS=$3

UUID=$(uuidgen)
VERSION=$(cat ../pom.xml | grep '<version>' | cut -d'>' -f 2 | cut -d'<' -f 1)

echo "INFO: Use pipelines version: ${VERSION}, UUID: ${UUID}, input: ${IN}, output: ${OUT}"

function download() {
  echo "Downloading ${VOCABULARY_NAME} vocabulary"
  curl -sS https://api.gbif.org/v1/vocabularies/${VOCABULARY_NAME}/releases/LATEST/export > configs/${VOCABULARY_NAME}.json
}


echo "Download vocabularies"
VOCABULARY_NAME=DegreeOfEstablishment
download
VOCABULARY_NAME=EstablishmentMeans
download
VOCABULARY_NAME=LifeStage
download
VOCABULARY_NAME=Pathway
download

VERBATIM_OUTPUT=${OUT}/${UUID}/1/verbatim.avro
./1-dwca-to-avro.sh ${VERSION} ${IN} ${VERBATIM_OUTPUT}

./2-interpretation.sh ${VERSION} ${UUID} ${VERBATIM_OUTPUT} ${OUT}

./3-indexing.sh ${VERSION} ${UUID} ${OUT} ${ES_HOSTS}

echo "INFO: End"
echo
echo "INFO: Check the ES index name index_name_example"
echo
