# Performing a full reinterpretation

These are the steps to reinterpret all occurrences, for example when taxonomy or geography lookups are changed.

1. Change **crawler-pipelines-index-dataset-\*.yaml** and set:
```
indexNumberReplicas: 0
indexAlias: occurrence_new
```
2. Change **crawler-pipelines-hdfs-view-\*.yaml** and set:
```
repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence_new
```
3. Create a new directory hdfs://ha-nn/data/hdfsview/occurrence_new and grant 777 permission, and snapshot creation permission
4. Stop crawling
5. Pause Oozie jobs – tables and maps
6. Deploy pipelines, crawler, registry, etc
7. Update **prodcrawlerN** configs
```
cd _github && git pull
```
8. Run crawler-pipelines-balancer, crawler-pipelines-interpret-dataset-\*, crawler-pipelines-index-dataset-\*, crawler-pipelines-hdfs-view-\* CLIs only
```
./start-pipelines-balancer
./start-pipelines-interpret-dataset-distributed
./start-pipelines-interpret-dataset-standalone
./start-pipelines-hdfs-view-distributed
./start-pipelines-hdfs-view-standalone
./start-pipelines-index-dataset-distributed
./start-pipelines-index-dataset-standalone
./start-pipelines-index-dataset-distributed
./start-pipelines-index-dataset-standalone
```
9. Run "interpret all datasets":
```
curl -iu username:password -X POST 'https://api.gbif-dev.org/v1/pipelines/history/run?steps=VERBATIM_TO_INTERPRETED&reason=Updated%20pipelines%20and%20index%20schema'
```
10. Wait until reprocessing has finished
11. Compare new and old indices to find missed data, fix data
12. Deploy the Oozie download workflow with schema migration: https://github.com/gbif/occurrence/blob/master/occurrence-download/run-workflow-schema-migration.sh
13. Swap the ES aliases. Delete **all** aliases from **all** indices and add **occurrence** alias to indices with the new suffix being used, for example **_20190926**
```
POST /_aliases
{
  "actions": [
    { "remove": { "index": "*", "alias": "*" } },
    { "add": { "index": "*_20190926*", "alias": "occurrence" } }
  ]
}
```
14. Deploy any new occurrence-ws
15. Tests that occurrence search, small downloads and big downloads work.
16. Remove hdfs://ha-nn/data/hdfsview/occurrence
17. Rename hdfs://ha-nn/data/hdfsview/occurrence_new to hdfs://ha-nn/data/hdfsview/occurrence
18. Enable snapshots for the new data folder
```
sudo -u hdfs hdfs dfsadmin -allowSnapshot /data/hdfsview/occurrence/
```
19. Remove the old pipelines tables
20. Deploy Oozie download workflow and build new download tables
21. Resume other Oozie jobs (maps)
22. Remove old indices. Delete **all** indices (first argument, `*`), except indices with suffix being used, for example **_20191001** (second argugent is `-*_20191001*`) and indices starting with **.** (third argument is `-.*`).
```
DELETE /*,-*_20191001*,-.*,-dataset*
```
23. Turn on replicas for the new index
```
PUT occurrence/_settings
{
  "index": { "number_of_replicas": 1 }
}
```
24. Change **crawler-pipelines-index-dataset-\*.yaml** and set:
```
indexNumberReplicas: 1
indexAlias: occurrence
```
25. Change **crawler-pipelines-hdfs-view-\*.yaml** and set:
```
repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence
```
26. Update **prodcrawler3** and **prodcrawler4** configs
```
cd _github && git pull
```
27. Restart pipelines CLIs
28. "Fix" downloads
29. Deploy the new portal version
