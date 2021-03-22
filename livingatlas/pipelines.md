# Pipelines

The livingatlas module contains a number of processes for ingest of data to support the 
living atlases. For details on deployment see the [architecture diagram](architectures.md).

The [la-pipelines](scripts/la-pipelines) command line tool is the mechanism for running these
pipelines either locally or on a spark cluster. This tool along with the YAML configuration files and built 
JAR file are deployed into production systems using a debian package 
(see [ansible scripts](https://github.com/AtlasOfLivingAustralia/ala-install/tree/27_pipelines_spark_hadoop/ansible/roles/pipelines) 
for details).  

This [la-pipelines](scripts/la-pipelines) tool uses configuration files in [configs/](configs)
directory to run the pipelines with as java processes or as jobs submitted to the spark cluster.

## DwCA to Verbatim

This is the first pipeline to run for a dataset. 
This takes a DwCA and converts to AVRO format using the 
[ExtendedRecord](../sdks/models/src/main/avro/extended-record.avsc) schema.

The result is a `verbatim.avro` file on the file system which is a verbatim representation
of the darwin core archive in AVRO.

Command to use:  `./la-pipelines dwca-avro dr123`

## Interpretation

The second pipeline that is run when ingesting a dataset. This can be run in 3 modes using the 
--mode flag:

* local - Use java only pipeline. This is best used for smaller datasets.
* embedded - Use embedded spark. This is for larger datasets when a cluster isn't available.
* cluster - Use a spark cluster. This is best used for large datasets. 

This pipeline generates AVRO outputs for each of the transforms which are stored in a directory
structure on the filesystem (HDFS or just a Unix filesystem). The pipeline will run the following transforms:
* basic
* taxonomy - use name matching service
* location
* attribution
* temporal
* multimedia

Pipeline class: [VerbatimToInterpretedPipeline](pipelines/src/main/java/au/org/ala/pipelines/beam/ALAVerbatimToInterpretedPipeline.java)

Command to use:  `./la-pipelines interpret dr123 --cluster`

## UUID

This pipeline is responsible for minting UUIDs on records.
UUIDs are created and stored in a subdirectory of the output for a dataset.
The pipeline will:
* Read information from the collectory to determine which fields can be used to 
  construct a unique key e.g. occurrenceID. 
* Check that all records have these fields and they are unique
* Load existing UUIDs for this dataset if it has been previously load
* Re-associate existing UUIDs with the records
* Mint UUIDs for new records
* Write the output to the filesystem

Pipeline class: [UUIDMintingPipeline](pipelines/src/main/java/au/org/ala/pipelines/beam/ALAUUIDMintingPipeline.java)

Command to use:  `./la-pipelines uuid dr123 --cluster`

## Sensitive data

Sensitive data service pipeline

Pipeline class: [InterpretedToSensitivePipeline](pipelines/src/main/java/au/org/ala/pipelines/beam/ALAInterpretedToSensitivePipeline.java)

Command to use: `./la-pipelines sds dr123 --cluster`

## Image

There are two pipelines used for the integration with the [image service](https://images.ala.org.au)

### Image load

This pipeline pushes new images to the image service for storage.
It can be run sync or async. When ran in async, the pipeline pushes image details
to the image service and returns, leaving the images to be loaded asynchronously
by the image service. 

Pipeline class: [ImageLoadPipeline](pipelines/src/main/java/au/org/ala/pipelines/beam/ImageServiceLoadPipeline.java)

Command to use:  `./la-pipelines image-load dr123 --cluster`

### Image sync

This pipeline retrieve details of the image stored in the image service for a dataset
and retrieves the identifiers (UUIDs) for these images so that they can be indexed
with the records.

Pipeline class: [ImageSyncPipeline](pipelines/src/main/java/au/org/ala/pipelines/beam/ImageServiceSyncPipeline.java)

Command to use:  `./la-pipelines image-sync dr123 --cluster`

## Index

This pipeline produces an AVRO representation of the record that will be sent
to an indexing platform such as SOLR.

This loads all the necessary AVRO files and 
creates AVRO files using the 
[IndexRecord](../sdks/models/src/main/avro/occurrence-index-record.avsc) schema.

Command to use:  `./la-pipelines index dr123 --cluster`

## Sampling

This pipeline provides the integration with the sampling service.
It will retrieve a unique set of coordinates for a dataset or
all datasets and retrieve values against spatial layers stored in 
the spatial service for environmental (raster) and contextual (polygon) layers.

It maintains a cache of these samples with is updated on each run.

Command to use:  `./la-pipelines sample dr123 --cluster`

## Jackknife

The pipeline runs environmental outlier detection.

Command to use:  `./la-pipelines jackknife --cluster`

## Clustering

The pipeline runs duplicate detection.

Command to use:  `./la-pipelines clustering --cluster`

## SOLR

The pipeline creates the SOLR index.

Command to use:  `./la-pipelines solr --cluster`