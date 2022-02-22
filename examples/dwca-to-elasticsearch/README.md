#### How to run:

_Remember! Project uses Java 8 version, make sure by runing `java -version` that you use correct java version_

1) You must build the project first by running build.sh script in the project root directory:

```shell
./build.sh
```

or use maven command:

```shell
mvn spotless:apply clean package -DskipTests -DskipITs -T 1C -PextraArtifacts
```

2) From dwca-to-elasticsearch directory run:

```shell
./dwca-to-elasticsearch.sh \
    /path_to_arhive/dwca.zip \
    /output_directory/dwca-to-es \
    http://ES_IP_NODE_1:9200,http://ES_IP_NODE_2:9200
```

where:
 - /path_to_arhive/dwca.zip - path to dwca archive
 - /output_directory/dwca-to-es - path to store mediator (avro, metrics, etc) files
 - http://ES_IP_NODE_1:9200,http://ES_IP_NODE_2:9200 - elasticseach hosts


Check the created index http://ES_IP_NODE_1:9200/index_name_example/_search
