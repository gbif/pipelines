#!/bin/bash -e

echo "INFO: Start"

./1-dwca-to-avro.sh

./2-interpretation.sh

./3-indexing.sh

echo "INFO: End"
