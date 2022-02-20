#!/bin/bash -e

echo "INFO: Start"

echo "INFO: Running 1-dwca-to-avro.sh"
./1-dwca-to-avro.sh

echo "INFO: Running 2-interpretation.sh"
./2-interpretation.sh

echo "INFO: Running 3-indexing.sh"
./3-indexing.sh

echo "INFO: End"
