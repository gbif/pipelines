1. Change **crawler-pipelines-index-dataset-\*.yaml** and set:
```
indexNumberReplicas: 0
indexAlias: occurrence_new
```
2. Change **crawler-pipelines-hdfs-view-\*.yaml** and set:
```
repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence_new
```
3. Create new direcoty hdfs://ha-nn/data/hdfsview/occurrence_new and give 777 permissions, and snapshot creation permission   
4. Stop crawling
5. Pause oozie jobs - tables and maps 
6. Deploy pipelines, crawler, registry, etc
7. Update **prodcrawlerN** configs
```
cd _github && git pull
```
8. Run crawler-pipelines-balancer, crawler-pipelines-interpret-dataset-*, crawler-pipelines-index-dataset-*, crawler-pipelines-hdfs-view-* CLIs only
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
curl POST -u username:password -X POST 'https://registry-api.gbif-dev.org/pipelines/history/run?steps=VERBATIM_TO_INTERPRETED&reason=Updated%20pipelines%20and%20index%20schema'
```
10. Wait until the finish
11. Compare new and old indices to find missed data, fix data
12. Run the Oozie Workflow with schema migration: https://github.com/gbif/occurrence/blob/master/occurrence-download/run-workflow-schema-migration.sh
13. Swap ES aliases. Delete **all** aliaces from **all** indices and add **occurrence** alias to indices with suffix being used, for example **_20190926**
```
POST /_aliases
{
  "actions": [
    { "remove": { "index": "*", "alias": "*" } },
    { "add": { "index": "*_20190926*", "alias": "occurrence" } }
  ]
}
```
14. Deploy new occurrence-ws
15. Tests that occurrence search, small downloads and big downloads work.
16. Remove hdfs://ha-nn/data/hdfsview/occurrence
17. Rename hdfs://ha-nn/data/hdfsview/occurrence_new to hdfs://ha-nn/data/hdfsview/occurrence
18. Remove old pipelines tables
19. Deploy oozie build table job and build new tables
20. Resume other oozie jobs
21. Remove old indices. Delete **all** (first arg is \*) indices, except indices with suffix being used, for example **_20191001** *(second arg is -\*_20191001\*)* and indices starting with **.** *(third arg is -.\*)*
```
DELETE /*,-*_20191001*,-.*,-dataset*
```
22. Turn on replicas for the new index
```
PUT occurrence/_settings
{
  "index": { "number_of_replicas": 1 }
}
```
23. Change **crawler-pipelines-index-dataset-\*.yaml** and set:
```
indexNumberReplicas: 1
indexAlias: occurrence
```
24. Change **crawler-pipelines-hdfs-view-\*.yaml** and set:
```
repositoryTargetPath: hdfs://ha-nn/data/hdfsview/occurrence
```
25. Update **prodcrawler3** and **prodcrawler4** configs
```
cd _github && git pull
```
26. Restart pipelines CLIs
27. "Fix" downloads
28. Deploy the new portal version
