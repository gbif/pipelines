#!/bin/bash -e

echo "INFO: Running 1-dwca-to-avro.sh"

IN=$1
OUT=$2

java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -cp ../../tools/archives-converters/target/archives-converters-2.11.6-SNAPSHOT-shaded.jar org.gbif.converters.DwcaToAvroConverter ${IN} ${OUT}
