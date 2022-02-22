#!/bin/bash -e

VERSION=$1
IN=$2
OUT=$3

echo "INFO: Running 1-dwca-to-avro.sh. Version: ${VERSION}, input: ${IN}, output: ${OUT}"

java -cp ../../tools/archives-converters/target/archives-converters-${VERSION}-shaded.jar org.gbif.converters.DwcaToAvroConverter ${IN} ${OUT}
