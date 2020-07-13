#!/usr/bin/env bash
rootDir=/data/biocache-load
# !!!!!! need to run this command on one vm then run the script !!!!
# rm -rf $rootDir/dr*.lockdir
echo "#### DWCA-AVRO #####"
SECONDS=0
tempDirs=()
drDwcaList=($(ls -S $rootDir/dr*/dr*.zip))
for drDwca in "${drDwcaList[@]}"; do
    filename=$(basename "$drDwca")
    datasetID="${filename%.*}"
    folder=$(dirname "$drDwca")
    if mkdir "$folder.lockdir"; then
      tempDirs+=("$folder.lockdir")
      # you now have the exclusive lock
      echo "[DWCA-AVRO] Starting dwca avro conversion for $datasetID....."
      ./dwca-avro.sh $datasetID
      echo "[DWCA-AVRO] Finished dwca avro conversion for $datasetID."
    else
      echo "[DWCA-AVRO] Skipping dwca avro conversion for $datasetID....."
    fi
done
duration=$SECONDS
echo "#### DWCA-AVRO - DWCA load of all took $(($duration / 60)) minutes and $(($duration % 60)) seconds."