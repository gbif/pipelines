#!/usr/bin/env bash
set -e
set -o pipefail

ENV=$1
TOKEN=$2

echo "Installing clustering workflow for $ENV"

echo "Get latest clustering config profiles from GitHub"
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/clustering/$ENV/clustering.properties

START=$(date +%Y-%m-%d)T$(grep '^startHour=' clustering.properties | cut -d= -f 2)Z
FREQUENCY="$(grep '^frequency=' clustering.properties | cut -d= -f 2)"
OOZIE=$(grep '^oozie.url=' clustering.properties | cut -d= -f 2)

# Gets the Oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=Clustering | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
# Oozie uses timezone UTC
mvn -Dclustering.frequency="$FREQUENCY" -Dclustering.start="$START" -DskipTests -Duser.timezone=UTC clean install

echo "Copy to Hadoop"
sudo -u hdfs hdfs dfs -rm -r /clustering-workflow/
sudo -u hdfs hdfs dfs -copyFromLocal target/clustering-workflow /
sudo -u hdfs hdfs dfs -copyFromLocal /etc/hive/conf/hive-site.xml /clustering-workflow/lib/

echo "Start Oozie clustering datasets job"
sudo -u hdfs oozie job --oozie $OOZIE -config clustering.properties -run
