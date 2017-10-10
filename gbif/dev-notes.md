## Elastic search

Notes while in early dev


Create the index:

```  
PUT /occurrence
{
  "settings" : {
        "index" : {
            "number_of_shards" : 54, 
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

On each machine:
sudo vi /etc/elasticsearch/elasticsearch.yml
Add:
  thread_pool.bulk.queue_size: 10000
  ensure that the data paths are using all the disks
sudo systemctl restart elasticsearch.service


Add a location as geoPoint

PUT _mapping/occurrence 
{
  "properties": {
    "message": {
      "type": "geo_point"
    }
  }
}


PUT occurrence/location/2
{
  "text": "Geo-point as a string",
  "location": "41.12,-71.34" 
}





DELETE http://c3n3.gbif.org:9200/occurrence/


PUT /occurrence
{
  "settings" : {
        "index" : {
            "number_of_shards" : 36, 
            "number_of_replicas" : 1,
            "refresh_interval" : "5m"
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
