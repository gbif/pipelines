#!/usr/bin/env bash
set -e
set -o pipefail

ENV=$1
TOKEN=$2

echo "Resuming clustering workflow for $ENV"

echo "Get latest clustering config profiles from GitHub"
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/clustering/$ENV/clustering.properties

OOZIE=$(grep '^oozie.url=' clustering.properties | cut -d= -f 2)

# Gets the Oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=Clustering | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Resuming current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $OOZIE -resume $WID
fi
