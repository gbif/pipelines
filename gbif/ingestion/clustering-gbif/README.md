## Occurrence Clustering

Provides utilities to cluster GBIF occurrence records.

Status: Having explored approaches using Spark ML (no success due to data skew) and
[LinkedIn ScANNs](https://github.com/linkedin/scanns) (limited success, needs more investigation)
this uses a declarative rule-based approach making use of domain knowledge. A multi-blocking stage
groups candidate record pairs, followed by a pair-wise comparison to detect links within the group.

The initial focus is on specimens to locate:
 1. Physical records (e.g. Isotypes and specimens split and deposited in multiple herbaria)
 2. Database duplicates across datasets (huge biases observed within datasets (e.g. gutworm datasets))
 3. Strong links between sequence records, citations and specimens

This is intended to be run regularly (e.g. daily) and therefore performance is of critical concern.

The output is bulk loaded into HBase, suitable for an API to deployed on.

Build the project: `mvn clean package`

To run (while in exploration - will be made into Oozie workflow later):

Setup hbase:
```
disable 'occurrence_relationships_experimental'
drop 'occurrence_relationships_experimental'
create 'occurrence_relationships_experimental',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
 {SPLITS => [
    '01','02','03','04','05','06','07','08','09','10',
    '11','12','13','14','15','16','17','18','19','20',
    '21','22','23','24','25','26','27','28','29','30',
    '31','32','33','34','35','36','37','38','39','40',
    '41','42','43','44','45','46','47','48','49','50',
    '51','52','53','54','55','56','57','58','59','60',
    '61','62','63','64','65','66','67','68','69','70',
    '71','72','73','74','75','76','77','78','79','80',
    '81','82','83','84','85','86','87','88','89','90',
    '91','92','93','94','95','96','97','98','99'
  ]}
```

Remove hive tables from the target database:
```
drop table occurrence_clustering_hashed;
drop table occurrence_clustering_candidates;
drop table occurrence_relationships;
```

Run the job (In production this configuration takes 2.6 hours with ~2.3B records)
```
nohup sudo -u hdfs spark2-submit --class org.gbif.pipelines.clustering.Cluster \
  --master yarn --num-executors 100 \
  --executor-cores 6 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=1000 \
  --executor-memory 64G \
  --driver-memory 4G \
  clustering-gbif-2.14.0-SNAPSHOT.jar \
  --hive-db prod_h \
  --hive-table-hashed occurrence_clustering_hashed \
  --hive-table-candidates occurrence_clustering_candidates  \
  --hive-table-relationships occurrence_relationships \
  --hbase-table occurrence_relationships_experimental \
  --hbase-regions 100 \
  --hbase-zk c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org \
  --hfile-dir /tmp/clustering &
```

Load HBase
```
sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dcreate.table=no \
  /tmp/clustering occurrence_relationships_experimental
```
