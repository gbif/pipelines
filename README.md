# Table of Contents

- [**About the project**](#about-the-project)
- [**Architecture**](#architecture)
    - [**Ingress**](#ingress)
    - [**Interpretation**](#interpretation)
    - [**Indexing**](#indexing)
- [**Structure**](#structure)
- [**How to build the project**](#how-to-build-the-project)
- [**Codestyle and tools**](#codestyle-and-tools)

# About the project

**Pipelines for data processing and indexing of biodiversity data**

_Status: WIP_

Vision: Consistent data processing pipelines (parsing, interpretation and quality flagging) for use in GBIF, the Living Atlases project and beyond.
Built to scale from laptop to GBIF volumes. Deployable on JVM, Spark, Google Cloud, <insert your favourite cloud provider here>.

# Architecture

The project provides vanilla JVM-based parsing and interpretation libraries, and pipelines for indexing into SOLR and ElasticSearch, built using Apache Beam.

> Apache Beam provides a high level abstraction framework ideal for this purpose with the ability to deploy across target environments (e.g. Spark, local JVM) and with many built in connectors for e.g. HBase, SOLR, ElasticSearch etc.

## Ingress

Ingress is from [Darwin Core Archive](https://www.tdwg.org/standards/dwc/) (zip files of one or more "CSV"s) or ABCD Archives (compressed XML) only[1].
During ingress data is converted from it's native format and stored as [Avro](https://avro.apache.org/docs/current/) files containing Darwin Core compliant data.

This is depicted below:

![Ingress](./docs/images/ingress.svg)

> Avro is chosen as a storage and interchange format exclusively in this project because a) it is splittable with each split compressed independently, b) it holds the data schema with the data, c) is well supported in the Hadoop ecosystem (e.g. Hive, Spark) and many other tools (e.g. Google cloud) d) is very robust in serialization and e) reduces boiler plate code thanks to schema to code generators. Darwin Core Archives and JSON for example do not exhibit all these traits.

[1] Other protocols (e.g. DiGIR, TAPIR) are supported by GBIF but are converted by crawling code upstream of this project.

## Interpretation

During interpretation the verbatim data is parsed, normalised and tested against quality control procedures.

To enable extensibility data is interpreted into separate [`avro`](https://avro.apache.org/docs/current/) files where a separate file per category of information is used.  Many interpretations such as `date / time` formating is common to all deployments, but not all.
For example, in the [GBIF.org](https://www.gbif.org) deployment the scientific identification is normalised to the GBIF backbone taxonomy and stored in `taxonomy/interpreted*.avro` which might not be applicable to everyone.
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

- [**.buildSrc**](./buildSrc) - Tools for building the project
- [**docs**](./docs) - Documents related to the project
- [**examples**](./examples) - Examples of using project API and base classes
- [**pipelines**](./pipelines) - Main pipelines module
    - [**beam-common**](./pipelines/beam-common) - Classes and API for using with Apache Beam
    - [**ingest-gbif**](./pipelines/ingest-gbif) - Main GBIF pipelines for ingestion biodiversity data
    - [**ingest-gbif-standalone**](./pipelines/ingest-gbif-standalone) - Independent GBIF pipelines for ingestion biodiversity data
    - [**ingest-transforms**](./pipelines/ingest-transforms) - Transformations for ingestion biodiversity data
- [**sdks**](./sdks) - Main module contains common classes, such as data models, data format iterpretations, parsers, web services clients ant etc.
    - [**core**](./sdks/core) - Main API classes, such as data interpretations, converters, [DwCA](https://www.tdwg.org/standards/dwc/) reader and etc
    - [**models**](./sdks/models) - Data models represented in Avro binary format, generated from [Avro](https://avro.apache.org/docs/current/) schemas
    - [**parsers**](./sdks/parsers) - Data parsers and converters, mainly for internal usage inside of interpretations
- [**tools**](./tools) - Module for different independent tools
    - [**archives-converters**](./tools/archives-converters) - Converters from [DwCA/DWC 1.0/DWC](https://www.tdwg.org/standards/dwc/) 1.4/ABCD 1.2/ABCD 2.06 to *.[avro](https://avro.apache.org/docs/current/) format
    - [**elasticsearch-tools**](./tools/elasticsearch-tools) - Tool for creating/deleting/swapping Elasticsearch indexes
    - [**pipelines-maven-plugin**](./tools/pipelines-maven-plugin) - Maven plugin adds new annotations and interface to [avro](https://avro.apache.org/docs/current/) generated classes

# How to build the project

The project uses [Apache Maven](https://maven.apache.org/) tool for building. Project contains maven wrapper and script for Linux and MacOS systems, you just can run **build.sh** script:

```./build.sh``` or ```source build.sh```

Please read [Apache Maven how-to](https://maven.apache.org/run.html).

# Codestyle and tools
### IntelliJ IDEA
- The simplest way to have uniform code style is to use the [Google Java Format plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format).
- Because project uses [Error-prone](https://code.google.com/p/error-prone) you may have issues during building process from IDEA, to avoid issues please install [Error-prone compiler integration plugin](https://plugins.jetbrains.com/plugin/7349-error-prone-compiler-integration) and allows to build project using [`error-prone java compiler`](https://code.google.com/p/error-prone) to catch common Java mistakes at compile-time. To use the compiler, go to File | Settings | Compiler | `Java Compiler` and select `Javac with error-prone` in `Use compiler` box.