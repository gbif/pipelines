# Migration of UUIDs

This module contains tools to assist the migration from biocache/cassandra stack to a pipelines based installation.

# Exporting UUIDs

An extract of unique keys to UUIDs and Unique Keys (i.e. the composite keys biocache constructs from 1 or more record fields)
can be created using the following CQL ran on a cassandra node:

```$xslt
COPY occ_uuid
TO '/data/occ_uuid.csv'
``` 

This file can then be copied to the spark cluster and imported into HDFS using

```$xslt
hdfs dfs -mkdir /migration
hdfs dfs -copyFromLocal /data/occ_uuid.csv /migration/occ_uuid.csv
```

The pipeline in this module creates a shaded jar that can be copied to the cluster
and ran using the bash scripts in the [scripts directory](scripts).

After running, the 

