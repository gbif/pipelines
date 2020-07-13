#!/usr/bin/env bash
source set-env.sh

java -cp $PIPELINES_JAR au.org.ala.utils.DumpDatasetSize \
--config=../configs/la-pipelines.yaml,../configs/la-pipelines-local.yaml

################################################################
# Step 1: Set UUIDs
################################################################

while IFS=, read -r datasetID recordCount
do
    echo "Dataset = $datasetID and count = $recordCount"
    if [ "$recordCount" -gt "50000" ]; then
      if [ "$USE_CLUSTER" == "TRUE" ]; then
        ./uuid-spark-cluster.sh $datasetID
      else
        ./uuid-spark-embedded.sh $datasetID
      fi
    else
      ./uuid-spark-embedded.sh $datasetID
    fi
done < /tmp/dataset-counts.csv
