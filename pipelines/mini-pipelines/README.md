# Mini pipelines

A collection of pipelines that run from a single jar for interpretation and indexing into Elasticsearch.
The pipelines make use of an embedded Spark instance to run.

## Main API classes:
 - [DwcaPipeline.java](./src/main/java/org/gbif/pipelines/minipipelines/DwcaPipeline.java)
 - [DwcaPipelineOptions.java](./src/main/java/org/gbif/pipelines/minipipelines/DwcaPipelineOptions.java)