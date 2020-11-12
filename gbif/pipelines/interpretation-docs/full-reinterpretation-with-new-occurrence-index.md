# Performing a full reinterpretation

These are the steps to reinterpret all occurrences, for example when taxonomy or geography lookups are changed.

- Change **crawler-pipelines-index-dataset-\*.yaml** and set:

    ```
    indexNumberReplicas: 0
    indexVersion: INDEX_VERSION
    indexAlias: occurrence_INDEX_VERSION
    ```
   
- Change **crawler-pipelines-hdfs-view-\*.yaml** and set:

    ```
    repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence_INDEX_VERSION
    ```
   
- Create a new directory hdfs://ha-nn/data/hdfsview/occurrence_INDEX_VERSION and grant 775 permission, and snapshot creation permission:

    ```
    sudo -u hdfs hdfs dfs -mkdir /data/hdfsview/occurrence_INDEX_VERSION
    sudo -u hdfs hdfs dfs -chmod -R 775 /data/hdfsview/occurrence_INDEX_VERSION
    ```
   
- Stop crawling and pause nagios alerts, ENVcrawlerN-vh.gbif.org:

    ```
    cd bin
    ./stop-all -d '1440 minutes'
    ```
   
- Pause Oozie jobs – tables and maps, cNgateway-vh.gbif.org:

    ```
    sudo -u hdfs oozie job -suspend JOB_COORDINATOR_ID -oozie http://cNmaster1-vh.gbif.org:11000/oozie/
    ```
   
- Deploy pipelines, crawler, registry, etc
- Update **ENVcrawlerN** configs, ENVcrawlerN-vh.gbif.org:

    ```
    cd _github && git pull
    ```
   
- Run crawler-pipelines-balancer, crawler-pipelines-interpret-dataset-\*, crawler-pipelines-index-dataset-\*, crawler-pipelines-hdfs-view-\* CLIs only:

    ```
    ./start-pipelines-balancer
    ./start-pipelines-interpret-dataset-distributed
    ./start-pipelines-interpret-dataset-standalone
    ./start-pipelines-hdfs-view-distributed
    ./start-pipelines-hdfs-view-standalone
    ./start-pipelines-index-dataset-distributed
    ./start-pipelines-index-dataset-standalone
    ```
   
- Run "interpret all datasets":

    ```
    curl -iu username:password -X POST 'https://api.gbif-ENV.org/v1/pipelines/history/run?steps=VERBATIM_TO_INTERPRETED&reason=YOUR%20MESSAGE'
    ```
   
- Wait for the process to complete
- Compare new and old indices to find missed data, [fix data](fix-failed-datasets.md):

    ```
    GET ALIAS/_search
    {
        "size": 0,
        "aggs": {
        "datasetKey_aggr": {
          "terms": {
            "field": "datasetKey",
            "size": 60000,
            "order" : { "_key": "asc" }
          }
        }
    }
    ```
  
- [Deploy the Oozie download workflow with schema migration](https://github.com/gbif/occurrence/blob/master/occurrence-download/run-workflow-schema-migration.sh):
- Swap the ES aliases. Delete **all** aliases from **all** indices and add **occurrence** alias to indices with the new version being used, for example **_a_**

    ```
    POST /_aliases
    {
      "actions": [
        { "remove": { "index": "*", "alias": "*" } },
        { "add": { "index": "*_INDEX_VERSION_*", "alias": "occurrence" } }
      ]
    }
    ```
    
- Deploy any new occurrence-ws
- Tests that occurrence search, small downloads and big downloads work.
- Rename hdfs://ha-nn/data/hdfsview/occurrence

    ```
    # List of snapshots
    sudo -u hdfs hdfs dfs -ls /data/hdfsview/occurrence/.snapshot
    
    # Delete snapshots
    sudo -u hdfs hdfs dfs -deleteSnapshot /data/hdfsview/occurrence/ SNAPSHOT_NAME
    
    # Disallow snaphots
    sudo -u hdfs hdfs dfsadmin -disallowSnapshot /data/hdfsview/occurrence
    
    # Move/Rename directory
    sudo -u hdfs hdfs dfs -mv /data/hdfsview/occurrence /data/hdfsview/occurrence_old
    ```
    
- Rename hdfs://ha-nn/data/hdfsview/occurrence_INDEX_VERSION to hdfs://ha-nn/data/hdfsview/occurrence and enable snapshots for the new data directory:
    ```
    # Rename
    sudo -u hdfs hdfs dfs -mv /data/hdfsview/occurrence_a /data/hdfsview/occurrence
    
    # Allow snapshots
    sudo -u hdfs hdfs dfsadmin -allowSnapshot /data/hdfsview/occurrence/
    ```
    
- Remove the old pipelines tables
- Deploy Oozie download workflow and build new download tables
- Resume other Oozie jobs (maps)
- Remove old indices. Delete **all** indices (first argument, `*`), except indices with version being used, for example **_a_** (second argugent is `-*_a_*`) and indices starting with **.** (third argument is `-.*`).:

    ```
    DELETE /*,-*_a_*,-.*,-dataset*
    ```
    
- Turn on replicas for the new index:

    ```
    PUT occurrence/_settings
    {
      "index": { "number_of_replicas": 1 }
    }
    ```
    
- Change **crawler-pipelines-index-dataset-\*.yaml** and set:

    ```
    indexNumberReplicas: 1
    indexAlias: occurrence
    ```
    
- Change **crawler-pipelines-hdfs-view-\*.yaml** and set:

    ```
    repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence
    ```
    
- Update **prodcrawler3** and **prodcrawler4** configs:

    ```
    cd _github && git pull
    ```
    
- Restart pipelines CLIs
- "Fix" downloads
- Deploy the new portal version
