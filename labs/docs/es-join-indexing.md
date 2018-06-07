# Introduction

This note shows the performance of doing join in Elastic search using partial updates. 

Two approaches were considered:
1. Partial Updates with flat json structures.
2. Partial Updates with nested json structures.

## Experiment setup
The experiment was run on development cluster (3 nodes 24 cores each and 62.71 GB RAM each)
1. Elastic Search 5.6.2 
2. Spark 2
3. Yarn
4. Apache Beam 2.4

###### Spark2 command
```spark2-submit --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=2048 --class <Pipelines> --master yarn --deploy-mode cluster --executor-memory 12G --executor-cores 8 --num-executors 16 --driver-memory 1G labs-1.1-SNAPSHOT-shaded.jar --datasetId=0645ccdb-e001-4ab0-9729-51f1755e007e --attempt=1 --coreSiteConfig=coresite.xml --hdfsSiteConfig=hdfs-site.xml --defaultTargetDirectory=hdfs://ha-nn/data/ingest/ --runner=SparkRunner```

###### ES Index settings
```
PUT /interpreted-dataset_0645ccdb-e001-4ab0-9729-51f1755e007e_1
{
	"settings": {
		"index": {
			"number_of_shards": <no_of_shards>,
			"number_of_replicas": 0,
			"refresh_interval": "-1",
			"translog.durability": "async"
		}
	},
	"mappings": {
		"type": {
			"dynamic_templates": [{
					"vocabularies": {
						"path_match": "*.id",
						"mapping": {
							"type": "keyword"
						}
					}
				},
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

PUT /interpreted-dataset_0645ccdb-e001-4ab0-9729-51f1755e007e_1/_mapping/interpreted_occurrence 
{
  "properties": {
    "id": {
      "type": "keyword"
    },
    "location": {
      "type": "geo_point"
    }
  }
}
```

## Results
Approaches |  shards | time | shards | time
-----------|---------|------|--------|------
Flat json structures|       9 shards|    21 mins|      72 shards|   17 mins 
Nested json structures| 9 shards  | 25 mins| 72 shards |20 mins


