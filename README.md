# Pipelines for data processing

_Experimental_ - please do not commit to this project without prior discussion.

This is an experimental space to explore Apache Beam for data pipelines for both GBIF and the Living Atlas project.

Topics of interest
 - data processing suitable for GBIF and ALA
 - simplified framework 
    - Avro schemas for messages (instead of GBIF API models)
    - Removal of messaging (no rabbit)
    - Simplified project structure (single project to release and maintain)    
 - consistent validation / flagging of record issues
 - flexible deployment options (Spark on YARN, Standalone Spark, Single machine, Google cloud etc)    
 - removal of HBase / Cassandra storage dependencies
    - generation of files (e.g. Avro) suitable for building e.g. SOLR / ES directly 
 - reduced boilerplate code to improve ability to react to change requests
  
The project is structured as:

- _core_: The schemas, messages and functions to interpret data
- _hadoop_: Hadoop specific utilities (e.g. FileInputFormats)
- _demo_: The quick start project (currently a demo showing a DwC-A to Avro file creation)
- _gbif_: The GBIF specific indexing pipelines (currently a demo showing an ES sink using Spark deployment)
- _living_atlases_: The Living Atlas specific indexing pipelines (currently a demo showing a SOLR sink)

