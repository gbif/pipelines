## Batch indexing into ElasticSearch from Avro

Notes collected while indexing into ES using the GBIF C4 cluster (masters + 9 nodes).

### Preconditions
These notes assume a large Avro file exists containing `ExtendedRecord` data.

On each ElasticSearch machine ensure the bulk queue size is suitable for bulk loading:
```
sudo vi /etc/elasticsearch/elasticsearch.yml
Add:
  thread_pool.bulk.queue_size: 10000
  ensure that the data paths are using all the disks
sudo systemctl restart elasticsearch.service
```
 
 
### Create the Index

Using no replicas for improved bulk load (they can be added later)

```  
PUT /occurrence
{
  "settings" : {
        "index" : {
            "number_of_shards" : 72, 
            "number_of_replicas" : 0,
             "refresh_interval" : "60s"
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

Add a location as geoPoint
```
PUT /occurrence/_mapping/occurrence 
{
  "properties": {
    "location": {
      "type": "geo_point"
    }
  }
}
```

To delete the index
```
DELETE occurrence
```

Example search:
```
GET occurrence/_search
{
  "size": 1, 
  "query": { 
    "match_phrase": { 
      "publishingCountry": { 
        "query": "US"
      }
    } 
  }
}
```

### Run the application

To launch indexing (see comments on tuning below):

```
sudo -u hdfs spark-submit \
  --master yarn \
  --conf spark.yarn.executor.memoryOverhead=2000 \
  --conf spark.default.parallelism=1000 \
  --executor-memory 5G \
  --executor-cores 5 \
  --num-executors 18 \
  --class org.gbif.pipelines.indexing.Avro2ElasticSearchPipeline \
  --runner=SparkRunner \
  --datasetId=tim \
  /tmp/gbif-pipelines-1.0-SNAPSHOT-shaded.jar  
```

### Tuning notes


| Param        | Notes | 
| ------------- |-------------| 
| `spark.yarn.executor.memoryOverhead` | To avoid YARN killing (`SIGTERM`) containers you can increase the offheap memory for it's overhead operations |
| `spark.default.parallelism` | Increasing this instructs Spark to use more partitions when reading the Avro file.  Having 1000 with a file of 500M records means 500,000 per partition.  More and smaller partitions reduces memory pressure on Spark and will help in avoiding Spark trying to use too much off heap memory (note Strings are off heap, and these are large String RDDs).  A values of 100 results in approx 5M records per partition and it has been observed this results in executors being killed by the YARN AM due to requesting too much offheap.  1000 has resulted in reasonably short running tasks (20 mins or so).  Higher values might yeild some stability benefit if necessary |
| `executorMemory` | The java heap for each executor shared by all the executor cores. 5GB with 6 cores has been observed to run without issue.  Note that unless `spark.yarn.executor.memoryOverhead` is provided the amount of offheap memory will be relative to the `executorMemory`. It is sensible to control both. |
| `num-executors` | The number of "JVMs" spawned by the Spark AM to accept work.  Each executor shares it's heap and offheap memory across the tasks it is running |
| `executor-cores` | The number of tasks that can concurrently be run per executor. |

As a guide for a sensible starting point on a 9 node cluster with relatively underpowered CPUs:

  - 72 cores in ES means 8 per machine.  Each machine has 12 disks so disk contention is no issue. We can assume 8 CPU cores will be utilised in ES.
  - Parallelism of 1000 means 0.1% of the source data will be used in each RDD partition.  With input Avro of 85GB we can assume approx. 850GB uncompressed meaning 0.85GB per partition.
  - At 0.85GB RDD partitions, and 5 cores we have a need for approx 4.25GB memory per executor (increased to 5GB for a buffer).
  - By monitoring CPU charts on Ganglia and throughput, running 72 clients to ES provides good stability (no timeouts etc).  Splitting these into 18 executors each with 5 cores has worked well.       
