# Ingest-GBIF standalone pipelines

A collection of pipelines that run from a single jar for interpretation and indexing into Elasticsearch.
The pipelines make use of an embedded Spark instance to run.

**REMEMBER, YOU HAVE TO USE  ONLY JAVA 8 VERSION**

## Main API classes:
 - [DwcaPipeline.java](./src/main/java/org/gbif/pipelines/standalone/DwcaPipeline.java)

## How to run:

Please change:
- **BUILD_VERSION** - is the current project version
- **DATASET_ID** - valid dataset id
- **ATTEMPT** - number of attempt
- **TARGET_PATH** - path to directory only
- **INPUT_PATH** - path to *.zip (in case of INTERPRETED_TO_ES_INDEX, path to root directory, the same as TARGET_PATH for DWCA_TO_INTERPRETED)
- **ES_HOSTS** - Elasticsearch URLs (http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200)
- **ES_INDEX_NAME** - Elasticsearch index name

#### From DwCA to ExtendedRecord *.avro file:
```shell
java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar \
 --runner=SparkRunner \
 --pipelineStep=DWCA_TO_VERBATIM \
 --datasetId=DATASET_ID \
 --attempt=ATTEMPT \
 --targetPath=TARGET_PATH \
 --inputPath=INPUT_PATH \
 --tempLocation=temp
```

#### From DwCA to GBIF interpreted *.avro files:
```shell
java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar \
 --runner=SparkRunner \
 --pipelineStep=DWCA_TO_INTERPRETED \
 --datasetId=DATASET_ID \
 --attempt=ATTEMPT \
 --targetPath=TARGET_PATH \
 --inputPath=INPUT_PATH \
 --tempLocation=temp
 --properties=/path/ws.properties
```

#### From DwCA to Elasticsearch index:
```shell
java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar \
 --runner=SparkRunner \
 --pipelineStep=DWCA_TO_ES_INDEX \
 --datasetId=DATASET_ID \
 --attempt=ATTEMPT \
 --inputPath=/some/path/to/input/dwca.zip \
 --esHosts=ES_HOSTS \
 --esIndexName=ES_INDEX_NAME \
 --tempLocation=temp
 --properties=/path/ws.properties
```

#### From GBIF interpreted *.avro files to Elasticsearch index:
```shell
java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar \
 --runner=SparkRunner \
 --pipelineStep=INTERPRETED_TO_ES_INDEX \
 --datasetId=DATASET_ID \
 --attempt=ATTEMPT \
 --inputPath=/some/path/to/input/pipelines/ \
 --esHosts=ES_HOSTS \
 --esAlias=ES_INDEX_NAME \
 --tempLocation=temp
 ```
