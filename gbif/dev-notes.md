## Elastic search

Notes while in early dev


``` 
create table occ_all stored as avro as
select
  cast(gbifid as String) as occurrenceId,
  v_kingdom as kingdom,
  v_phylum as phylum,
  v_class as class,
  v_order as `order`,
  v_family as family,
  v_genus as genus,
  species,
  v_specificepithet as specificEpithet,
  v_infraspecificepithet as infraspecificEpithet,
  v_taxonrank as taxonRank,
  v_scientificname as scientificName,
  v_scientificnameauthorship as scientificNameAuthorship,
  v_basisofrecord as basisOfRecord,
  v_decimallatitude as decimalLatitude,
  v_decimallongitude as decimalLongitude,
  v_geodeticdatum as geodeticDatum,
  v_country as country,
  v_eventdate as eventDate,
  v_year as year,
  v_month as month,
  v_day as day
from dev.occurrence_hdfs
```


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

PUT /occurrence/_mapping/occurrence 
{
  "properties": {
    "location": {
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





POST /occurrence/_search
{
   "query": {
      "range": {
         "decimalLongitude": {
            "from": 23.4,
            "to": 91.8
         }
      }
   }
}

POST /occurrence/_search
{
   "query": {
      "match": {
         "kingdom.string": {
            "query": "Animalia",
            "type": "phrase"
         }
      }
   },
   "aggs": {
      "basisOfRecords": {
         "terms": {
            "field": "basisOfRecord.string.keyword",
            "min_doc_count": 1
         }
      }
   }
}


POST /occurrence/_search
{
   "query": {
       "match_all": {}
   }
}

POST /occurrence/_search
{
  "size": 0,
  "aggs": {
        "basisOfRecords": {
          "terms": {
            "field": "basisOfRecord.string.keyword",
            "min_doc_count": 1
          }
        }
      }
}
