## How to use:

_Note:_ To assemble diagnostics tool artifact you have to activate maven profile
```shell
mvn clean package -P extra-artifacts
```


Diagnostics tool contains 3 key features:
- MIGRATOR - when you need to migrate GBIF identifiers from old occurrence_id/triplet to the new occurrence_id/triplet
- REPAIR - (DEPRECATED) fixing GBIF ID collisions by removing GBIF identifier value from triplet or occurrence_id
- LOOKUP - when you want to print out the values in occurrenceID and the triplet in each crawl attempt DwC-A


#### Display HELP information

```shell
java -jar target/diagnostics-VERSION-SNAPSHOT-shaded.jar --help
```

#### MIGRATOR

**Migrator source file** - By defualt file format is CSV, the file must be without header and contain two rows with values, where first value is old_occurrence_id, second is the new_occurrence_id, exaple:
```csv
0000001,GLM-P-0000001
0000002,GLM-P-0000002
0000003,GLM-P-0000003
```
_Note:_ that it is possible to migrate GBIF identifiers using triplet value, the file must have the following format:
```csv
old_institutionCode|old_collectionCode|old_catalogNumber|null,new_institutionCode|new_collectionCode|new_catalogNumber|null
```
```csv
SMNG|GLM|1|null,SMNG|GLM|GLM-P-0000001|null
SMNG|GLM|2|null,SMNG|GLM|GLM-P-0000002|null
SMNG|GLM|3|null,SMNG|GLM|GLM-P-0000003|null
```


Keys:
- tool - name for the tool, must be MIGRATOR
- zk-connection - Zookeeper connection string for HBase
- lookup-table - HBase lookup table name
- counter-table - HBase counter table name
- occurrence-table - HBase occurrence table name
- from-dataset - Registry source datasetKey (usually identical for from-dataset and to-dataset keys) 
- to-dataset - Registry new datasetKey (usually identical for from-dataset and to-dataset keys)
- file-path - Path to the csv file
- delete-keys - (Optional) Deletes GBIF identifiers if they have been created for new occurrence_id 
- skip-issues - (Optional) Continue the processing when an issue appears
- splitter - (Optional) Default is comma (,)

```shell
java -jar target/diagnostics-VERSION-SNAPSHOT-shaded.jar \
  --tool migrator \
  --from-dataset ${DATASET_FROM} \
  --to-dataset ${DATASET_TO} \
  --file-path ${FILE_PATH} \
  --zk-connection ${ZK_ENV} \
  --lookup-table ${LOOKUP_TABLE} \
  --counter-table ${COUNTER_TABLE} \
  --occurrence-table ${OCCURRENCE_TABLE} \
  --delete-keys \
  --skip-issues
```

#### REPAIR - (DEPRECATED)
The feature is deprecated, becuase current GBIF identifier workflow doesn't rely on couple triplets+occurrence_id, but only occurrence_id

```shell
java -jar target/diagnostics-VERSION-SNAPSHOT-shaded.jar \
  --dataset-key DATASET_REGISTY_KEY \
  --input-source /full/path/DATASET_REGISTY_KEY/DATASET_REGISTY_KEY.dwca \
  --zk-connection ZK_CONNECTION_STRING \
  --lookup-table LOOKUP_TABLE \
  --counter-table COUNTER_TABLE \
  --occurrence-table OCCURRENCE_TABLE \
  --deletion-strategy BOTH \
  --only-collisions \
  --dry-run
```

#### LOOKUP

```shell
java -jar target/diagnostics-VERSION-SNAPSHOT-shaded.jar --help
```
