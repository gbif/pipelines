### How to run:

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


### Fix possible issues:
If you use MacOS you may see the issue during the building process

```shell
[ERROR] Failed to execute goal on project archives-converters: Could not resolve dependencies for project org.gbif.pipelines:archives-converters:jar:2.11.7-SNAPSHOT: Could not find artifact com.sun:tools:jar:1.7.0 at specified path /Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home/../lib/tools.jar -> [Help 1]
```

Which means that by default JAVA_HOME points to JRE instead of JDK, you need to check versions:

```shell
/usr/libexec/java_home -V

Matching Java Virtual Machines (3):
    11.0.5 (x86_64) "Oracle Corporation" - "Java SE 11.0.5" /Library/Java/JavaVirtualMachines/jdk-11.0.5.jdk/Contents/Home
    1.8.291.10 (x86_64) "Oracle Corporation" - "Java" /Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home
    1.8.0_291 (x86_64) "Oracle Corporation" - "Java SE 8" /Library/Java/JavaVirtualMachines/jdk1.8.0_291.jdk/Contents/Home
```

and export necessary JDK path manually:

```shell
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_291.jdk/Contents/Home
```

run the build command
