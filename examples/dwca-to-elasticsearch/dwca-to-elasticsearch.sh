#!/bin/bash -e

echo "INFO: Start"

IN=$1
OUT=$2

UUID=$(uuidgen)
VERSION=$(cat ../pom.xml | grep '<version>' | cut -d'>' -f 2 | cut -d'<' -f 1)

echo "INFO: Use pipelines version: ${VERSION}, UUID: ${UUID}, input: ${IN}, output: ${OUT}"

VERBATIM_OUT=${OUT}/${UUID}/verbatim.avro
./1-dwca-to-avro.sh ${VERSION} ${IN} ${VERBATIM_OUT}

./2-interpretation.sh ${VERSION}

./3-indexing.sh ${VERSION}

echo "INFO: End"
