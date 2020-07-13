#!/usr/bin/env bash
source set-env.sh

java -cp $PIPELINES_JAR au.org.ala.utils.DumpDatasetSize \
--config=../configs/la-pipelines.yaml,../configs/la-pipelines-local.yaml

################################################################
# Step 1: Export lat lngs
################################################################

while IFS=, read -r datasetID recordCount
do
    echo "Dataset = $datasetID and count = $recordCount"
    if [ "$recordCount" -gt "50000" ]; then
      if [ "$USE_CLUSTER" == "TRUE" ]; then
        ./export-latlng-cluster.sh $datasetID
      else
        ./export-latlng-embedded.sh $datasetID
      fi
    else
      ./export-latlng-embedded.sh $datasetID
    fi
done < /tmp/dataset-counts.csv

################################################################
# Step 2: Sampling
################################################################

while IFS=, read -r datasetID recordCount
do
    ./sample.sh $datasetID
done < /tmp/dataset-counts.csv

################################################################
# Step 3: Write sampling to AVRO
################################################################

while IFS=, read -r datasetID recordCount
do
    echo "Dataset = $datasetID and count = $recordCount"
    if [ "$recordCount" -gt "50000" ]; then
      if [ "$USE_CLUSTER" == "TRUE" ]; then
        ./sample-avro-cluster.sh $datasetID
      else
        ./sample-avro-embedded.sh $datasetID
      fi
    else
      ./sample-avro-embedded.sh $datasetID
    fi
done < /tmp/dataset-counts.csv