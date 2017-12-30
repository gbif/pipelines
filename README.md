# Pipelines for data processing and indexing

_Status: early development_

Vision: Consistent data processing pipelines (parsing, interpretation and quality flagging) for use in GBIF, the Living Atlases project and beyond. 
Built to scale from laptop to GBIF volumes.  Deployable on JVM, Spark, Google Cloud, <insert your favourite cloud provider here>.

# Architecture

The project provides vanilla JVM-based parsing and interpretation libraries, and pipelines for indexing into SOLR and ElasticSearch, built using Apache Beam.  

> Apache Beam provides a high level abstraction framework ideal for this purpose with the ability to deploy across target environments (e.g. Spark, local JVM) and with many built in connectors for e.g. HBase, SOLR, ElasticSearch etc.

## Ingress

Ingress is from Darwin Core Archive (zip files of one or more "CSV"s) or  ABCD Archives (compressed XML) only[1].
During ingress data is converted from it's native format and stored as Avro files containing Darwin Core compliant data.  

This is depicted below: 

![Ingress](./docs/images/ingress.svg)
  
> Avro is chosen as a storage and interchange format exclusively in this project because a) it is splittable with each split compressed independently, b) it holds the data schema with the data, c) is well supported in the Hadoop ecosystem (e.g. Hive, Spark) and many other tools (e.g. Google cloud) d) is very robust in serialization and e) reduces boiler plate code thanks to schema to code generators. Darwin Core Archives and JSON for example do not exhibit all these traits.
  
[1] Other protocols (e.g. DiGIR, TAPIR) are supported by GBIF but are converted by crawling code upstream of this project.

## Interpretation

During interpretation the verbatim data is parsed, normalised and tested against quality control procedures.  

To enable extensibility data is interpreted into separate `avro` files where a separate file per category of information is used.  Many interpretations such as `date / time` formating is common to all deployments, but not all.  
For example, in the [GBIF.org](https://www.gbif.org) deployment the scientific identification is normalised to the GBIF backbone taxonomy and stored in `gbif-taxonomy.avro` which might not be applicable to everyone. 
Separating categories allows for extensibility for custom deployments in a reasonably modular fashion.  

Interpretation is depicted below:

![Ingress](./docs/images/interpret.svg)

> Note that all pipelines are designed and tested to run with the `DirectRunner` and the `SparkRunner` at a minimum.  This allows the decision to be taken at runtime to e.g. opt to interpret a small dataset in the local JVM without needing to use cluster resources for small tasks.

> It is a design decision to ensure that all the underlying parsers are as reusable as possible for other projects with careful consideration to not bring in dependencies such as Beam or Hadoop.

## Indexing

Initial implementations will be available for both SOLR and for ElasticSearch to allow for evaluation of both at GBIF.
During indexing the categories of interpreted information of use are merged and loaded into the search indexes:

![Ingress](./docs/images/index.svg)

> Note that GBIF target 10,000 records/sec per node indexing speed (i.e. 100,000 records/sec on current production cluster).  This will allow simplified disaster recovery and rapid deployment and of new features.

# Structure
  
The project is structured as:

- _core_: The schemas, messages and functions to interpret data
- _hadoop_: Hadoop specific utilities (e.g. FileInputFormats)
- _demo_: The quick start project (currently a demo showing a DwC-A to Avro file creation)
- _gbif_: The GBIF specific indexing pipelines (currently a demo showing an ES sink using Spark deployment)
- _living_atlases_: The Living Atlas specific indexing pipelines (currently a demo showing a SOLR sink)





