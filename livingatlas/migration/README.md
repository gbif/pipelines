# Migration of UUIDs

This module contains tools to assist the migration from biocache/cassandra stack to the
 pipelines based installation.

# Exporting UUIDs

An extract of unique keys to UUIDs, Unique Keys (i.e. the composite keys biocache constructs from 1 or more record fields)
and firstLoadedDate can be created using the following CQL ran on a cassandra node:

```$xslt
cqlsh> use occ;
cqlsh> COPY occ_uuid TO '/data/cass-export/occ_uuid.csv';
cqlsh> COPY occ (rowkey, "firstLoaded") TO '/data/cass-export/occ_first_loaded_date.csv';
``` 
The COPY exports currently take about 13 mins and 17mins to run (90 million records).

These files can then be copied to the spark cluster and imported into HDFS using:

```$xslt
hdfs dfs -mkdir /migration
hdfs dfs -copyFromLocal /data/occ_uuid.csv /migration/occ_uuid.csv
hdfs dfs -copyFromLocal /data/occ_first_loaded_date.csv /migration/occ_first_loaded_date.csv
```

The spark based pipeline in this module creates a shaded jar that can be copied to the cluster
and ran using the bash scripts in the [scripts directory](scripts).

After running this migration pipeline, the `ALAUUIDMintingPipeline` needs to be ran for the 
UUIDs to be picked up and used with the data.

