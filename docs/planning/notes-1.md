# Project planning notes

_status: for review by F.M._

The following list a proposed set of milestones for first development.  
Once reviewed and amended they should be discussed in a initial kickoff, and then converted into GH Milestones and issues.

This assumes the general outline setout in [the project readme](../../README.md) is a reasonable first aim.

## Milestone 1 (GBIF): DwC-A Ingress
A pipeline exists to write a DwC-A into verbatim Avro files.
The pipeline can be run locally or on Spark (saving to HDFS) and unit / integration tests are in place. This will set the preliminary standard for all future work.
The basic development structure (e.g. style guidelines, coding conventions) and process (e.g. peer review, demonstrations) are in place.

## Milestone 2 (GBIF): ABCD Ingress
ABCD-A are written into verbatim Avro files (storing an XML snippet, and Darwin Core verbatim terms). Code exists for BioCASe endpoints to be crawled and converted into ABCD-A files but is not hooked up to any live crawling.

## Milestone 3 (GBIF): Basic interpretation (flat data)
Verbatim Avro files can be interpreted into:
 - Temporal data into an Avro file (temporal.avro) 
   - should include an issue linking all the known problems, and result in both code and documentation on the rules chosen
 - Spatial data into an Avro file (gbif-location.avro) - "gbif" because it will use GBIF spatial gazeteers
 - Scientific identification parsed and aligned with the GBIF backbone (gbif-taxonomy.avro)
 - The occurrence issues used in the GBIF V1 (required for backwards compatibility) into an Avro file (gbif-issues.avro)

These files are stored into a directory for the dataset either on the local file system or on HDFS.

Note: The Avro schemas will be preliminary at this stage, and future iterations will introduce 
  1. Quality control flags (an evolution of the GBIF "issue" flags)
  2. Statements about how the data has been interpreted to aid debugging and ensure transparency (i.e. will add data lineage)

## Milestone 4 (GBIF): SOLR indexing (first iteration)
The interpreted data from Milestone 3 is written into a flat SOLR schema.
An understanding of the following is captured in the `docs` folder using Markdown:
 - throughput of indexing (records/sec/node) documented for C4 and C5 class hardware for SOLR on HDFS and SOLR on local filesystem
 - affect of number of shards for indexing throughput
 - affect of number of disks used for indexing throughput (when not using HDFS)
 - affect on indexing throughput and index size when data is stored in SOLR 

A basic set of queries covering small and large result sets, and facets is run on each set up and documented. 
Note: this will be repeated on Elasticsearch.

## Milestone 5 (GBIF): Elasticsearch indexing (first iteration)
The interpreted data from Milestone 3 is written into ES.
An understanding of the following is captured in the `docs` folder:
 - throughput of indexing (records/sec/node) documented for C4 and C5 class hardware 
 - affect of number of shards for indexing throughput
 - affect of number of disks used for indexing throughput 
An understanding of throughput of indexing (records/sec/node) is documented for C4 and C5 class hardware .

The standard query tests will be run on Elasticsearch to evaluate the performance in response.

## Milestone 6 (GBIF): Elasticsearch and Hive tests

Tests are performed and documented in the `docs` folder using Markdown.

The vector tile server map test will be deployed on the large C4 and C5 deployments of Elasticsearch with the `location` field populated.
A document of a test procedure capturing response times for z0, z4 and z10 tiles is run for both small result sets (e.g. _Felidae_ between 1970-2011) and for large result sets (e.g. _Aves_ between 1970-2018).

Hive on Elasticsearch is tested as a candidate to allow all egress (downloads) to be built using Hive and deployed against Avro or Elasticsearch.
Hive is tested for performance of small downloads when using Spark and running against Avro files, Parquet / ORC files (build from the Avro versions).  Reuse of Spark sessions is explored for performance for various Spark cluster sizes, and the feasibility of doing this will be explored (e.g. implications of dynamic allocation in Cloudera).

## Milestone 7 (GBIF): Software and project are presentable
Code is clean, branches are tidy, the Readme's are all in place, dependencies are to the latest desired version, issues are clear and categorised.
(This milestone exists to remind us to periodically burn off the technical debt accumulated and ensure a high quality project)

## Milestone 8 (GBIF): Extension data is interpreted 
Building on Milestone 4, the extension data supported by GBIF in production is interpreted, stored and available in SOLR / Elasticsearch (both or only one if it is clear that one technology is preferred).

## Milestone 9 (GBIF): Build GBIF SOLR schema to support (v1 search API) for backwards compatibility.
This might be reconsidered, but at the time of writing I believe this will be required and hopefully can be easily built using the work up to Milestone 7 and Beam.
