### Elasticsearch(v5.6.2) indexing pipeline using CoGroupByKey Beam's functionality - ElasticCoGroupByKeyPipeline.java

#### 1) Create an index (Kibana dev tools query):
* `number_of_shards` - Looks like 3 per node is enough
* `number_of_replicas` - Zero is only for testing
* `refresh_interval` - Gives some performance, but not so much
```
PUT co-group-idx
{
  "settings": {
    "index": {
      "number_of_shards": 9,
      "number_of_replicas": 0,
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "type": {
      "dynamic_templates": [
        {
          "strings": {
            "match_mapping_type": "string",
            "mapping": {
              "type": "keyword"
            }
          }
        }
      ]
    }
  }
}
```

#### 2) How to run a spark job:
* `spark.executor.memoryOverhead` and `executor-memory` - Most important parameters, can increase or decrease job time, for c3 and c4, I allocated the maximum
* `executor-cores` and `um-executors` - Not so important
* `ESMaxBatchSize` - Default value is 1000, must be enough
```
sudo -u hdfs spark2-submit
--conf spark.default.parallelism=500
--conf spark.executor.memoryOverhead=4096
--class org.gbif.pipelines.labs.cogroupbykey.ElasticCoGroupByKeyPipeline
--master yarn
--executor-memory 10G
--executor-cores 6
--num-executors 12
--driver-memory 1G /home/crap/lib/labs-1.1-SNAPSHOT-shaded.jar
--datasetId=38b4c89f-584c-41bb-bd8f-cd1def33e92f
--attempt=149
--runner=SparkRunner
--defaultTargetDirectory=hdfs://ha-nn/data/ingest/38b4c89f-584c-41bb-bd8f-cd1def33e92f/149/
--inputFile=hdfs://ha-nn/data/ingest/38b4c89f-584c-41bb-bd8f-cd1def33e92f/149/
--hdfsSiteConfig=/home/crap/config/hdfs-site.xml
--coreSiteConfig=/home/crap/config/core-site.xml
--ESHosts=http://c3n1.gbif.org:9200,http://c3n2.gbif.org:9200,http://c3n3.gbif.org:9200
--ESIndex=co-group-idx
--ESType=co-group-idx
--ESMaxBatchSize=1000
```

#### 3) Query the results (Kibana dev tools query):
```
GET co-group-idx/_search
```

#### 4) Delete the index (Kibana dev tools query):
```
DELETE co-group-idx
```