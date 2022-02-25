#!/bin/bash -e

VERSION=$1
IN=$2
OUT=$3

echo
echo "INFO: Running 1-dwca-to-avro.sh. Version: ${VERSION}, input: ${IN}, output: ${OUT}"
echo

java -XX:+PerfDisableSharedMem -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+UseCMSInitiatingOccupancyOnly -Xms128M -cp ../../tools/archives-converters/target/archives-converters-${VERSION}-shaded.jar org.gbif.converters.DwcaToAvroConverter ${IN} ${OUT}
