# Elasticsearch Mappings
The mappings file [occurrence_mappings.json](occurrence_mappings.json) contains the main dynamic schema definition to create an ES index.
To create an Index use the command:
 ```bash
 curl -X PUT http://elasticsearchserver:9200/occurrence -d @occurrence_mappings.json
 ```

