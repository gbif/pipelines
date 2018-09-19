# Ingest-GBIF standalone pipelines

A collection of pipelines that run from a single jar for interpretation and indexing into Elasticsearch.
The pipelines make use of an embedded Spark instance to run.

## Main API classes:
 - [DwcaPipeline.java](./src/main/java/org/gbif/pipelines/standalone/DwcaPipeline.java)

## Pipeline options:

#### From DwCA to ExtendedRecord *.avro file:
```
 --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 --attempt=1
 --pipelineStep=DWCA_TO_AVRO
 --targetPath=/some/path/to/output/
 --inputPath=/some/path/to/input/dwca/dwca.zip
```

#### From DwCA to GBIF interpreted *.avro files:
```
 --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 --attempt=1
 --pipelineStep=INTERPRET
 --targetPath=/some/path/to/output/
 --inputPath=/some/path/to/input/dwca/dwca.zip
```

#### From DwCA to Elasticsearch index:
```
 --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 --attempt=1
 --pipelineStep=INDEX_TO_ES
 --inputPath=/some/path/to/input/dwca.zip
 --esHosts=http://ADDRESS,http://ADDRESS,http://ADDRESS:9200
 --esIndexName=pipeline
```

#### From GBIF interpreted *.avro files to Elasticsearch index:
```
 --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 --attempt=1
 --pipelineStep=INTERPRET_TO_INDEX
 --inputPath=/some/path/to/input/pipelines/
 --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200
 --esIndexName=pipeline
 ```