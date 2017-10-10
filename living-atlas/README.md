# Living Atlas Pipelines

Status: For demonstration only!
 
Prerequisites: A running SOLR 5.x cluster (see the [solr](solr) directory with the schema etc).

To build:
```
mvn package
```

Note: a lot of hard coded paths etc in the software.

Pipelines:
 - One pipeline takes a DwC-A file and load it into SOLR running locally (e.g. in an IDE).

To run:
Edit the hardcoded locations in `DwCA2SolrPipeline` and run the class in your favourite IDE.
