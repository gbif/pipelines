## Occurrence Clustering

Processes occurrence data and establishes links between similar records. 

The output is a set of HFiles suitable for bulk loading into HBase which drives the pipeline lookup and 
public related occurrence API.

Build the project: `mvn spotless:apply test package -Pextra-artifacts`

To run this against a completely new table:

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

Run the job  
(In production this configuration takes ~2hours with ~2.3B records)
```
hdfs dfs -rm -r /tmp/clustering

nohup sudo -u hdfs spark2-submit --class org.gbif.pipelines.clustering.Cluster \
  --master yarn --num-executors 100 \
  --executor-cores 4 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=1200 \
  --executor-memory 64G \
  --driver-memory 4G \
  --conf spark.executor.memoryOverhead=4096 \
  --conf spark.debug.maxToStringFields=100000 \
  --conf spark.network.timeout=600s \
  clustering-gbif-2.18.0-SNAPSHOT.jar \
  --hive-db prod_TODO \
  --source-table occurrence \
  --hive-table-prefix clustering \
  --hbase-table occurrence_relationships_experimental \
  --hbase-regions 100 \
  --hbase-zk c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org \
  --target-dir /tmp/clustering \
  --hash-count-threshold 100 &
```

Load HBase
```
sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dcreate.table=no \
  /tmp/clustering occurrence_relationships_experimental
```
