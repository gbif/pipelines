# The example demonstrates how to create and send Apache Beam SparkRunner metrics to ELK and use the result for Kibana dashboards

## Main objective
[ELK](https://www.elastic.co/elk-stack)
---

### 1. Add SLF4J realisation for Spark

#### 1.1 Dependencies:

#### 1.2 Slf4J class realisation:

[Slf4jSink.java](./src/main/java/org/gbif/pipelines/common/beam/Slf4jSink.java)

```java
package org.gbif.pipelines.common.beam;

import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import org.apache.beam.runners.spark.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.metrics.WithMetricsSupport;

/**
 * A Spark {@link org.apache.spark.metrics.sink.Sink} that is tailored to report {@link
 * AggregatorMetric} metrics to Slf4j.
 */
public class Slf4jSink extends org.apache.spark.metrics.sink.Slf4jSink {
  public Slf4jSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    super(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
  }
}
```

#### 1.3 Add metrics to a pipeline:

#### 1.4 Add an additional information to the logger:
---

### 2. Provide Spark metrics and logger configurations

#### 2.1 Create Spark metrics configuration file

Please read about [Spark metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics)

[metrics.properties](./src/main/resources/metrics.properties)
```properties
executor.sink.slf4j.class=org.apache.spark.metrics.sink.Slf4jSink
driver.sink.slf4j.class=org.gbif.pipelines.common.beam.Slf4jSink
```

#### 2.2 Create Spark log4j configuration file
[log4j.properties](./src/main/resources/log4j.properties)
```properties
```
---

### 3. Create Logstash main configuration
Create a simple Logstash configuration file and call it ```examples-metrics.config```. For more detailed information please read articles [Logstash configuration file structure](https://www.elastic.co/guide/en/logstash/current/configuration-file-structure.html) and [more complex examples](https://www.elastic.co/guide/en/logstash/current/config-examples.html)

#### 3.1 Input section
Add input section to listen the host and port for [GELF(The Graylog Extended Log Format)](http://docs.graylog.org/en/2.4/pages/gelf.html) messages
```
input {
    gelf {
        host => "127.0.0.1"
        port => "12201"
    }
}
```

#### 3.1 Filter section
In our case necessary information about metrics is stored in ```message``` field.
We can add filter section with [kv](https://www.elastic.co/guide/en/logstash/current/plugins-filters-kv.html#plugins-filters-kv), kv helps automatically parse messages and convert from string to json.

Before kv filter:
```
"message": "type=GAUGE, name=local-1538473503470.driver.MetricsPipeline.Beam.Metrics.Counts_quantity_using_metrics_ParMultiDo_FilterText.org.gbif.pipelines.examples.MetricsPipeline.foo, value=41.0"
```

After kv filter:
```json
"message": {
    "type": "GAUGE",
    "name": "local-1538473503470.driver.MetricsPipeline.Beam.Metrics.Counts_quantity_using_metrics_ParMultiDo_FilterText.org.gbif.pipelines.examples.MetricsPipeline.foo",
    "value": "41.0"
}
```

- ```source``` - a field for parsing
- ```target``` - a new field for parsed result
- ```field_split``` - split characters, in our case it is a comma
- ```trim_key``` - to remove spaces in a key
```
filter {
    kv {
        source => "message"
        target => "messageKv"
        field_split => ","
        trim_key => " "
    }
}
```

#### 3.2 Output section
To output in
```
output {
    stdout {
        codec => "rubydebug"
    }
    elasticsearch {
        hosts => "localhost:9200"
        index => "examples-metrics-%{+YYYY.MM.dd}"
    }
}
```

Completed configuration:
```
input {
    gelf {
        host => "127.0.0.1"
        port => "12201"
    }
}

filter {
    kv {
        source => "message"
        target => "messageKv"
        field_split => ","
        trim_key => " "
    }
}

output {
    stdout {
        codec => "rubydebug"
    }
    elasticsearch {
        hosts => "localhost:9200"
        index => "examples-metrics-%{+YYYY.MM.dd}"
    }
}
```
---

### 4. How to run the example

```shell
elasticsearch/bin/elasticsearch
```

```shell
kibana/bin/kibana
```

```shell
logstash/bin/logstash -f examples-metrics.config
```

Standalone Spark
```shell
java -jar target/examples-metrics-BUILD_VERSION-shaded.jar src/main/resources/example.properties
```

Cluster Spark
```shell
sudo -u hdfs spark2-submit --conf spark.metrics.conf=metrics.properties  --conf "spark.driver.extraClassPath=/home/crap/logstash-gelf-1.11.2.jar" --driver-java-options "-Dlog4j.configuration=file:/log4j.properties" --class org.gbif.pipelines.examples.MetricsPipeline --master yarn examples-metrics-BUILD_VERSION-shaded.jar src/main/resources/example.properties
```
---

### 5. Change logging template index
[Logstash mapping](https://www.elastic.co/blog/logstash_lesson_elasticsearch_mapping)

---
### 6. Create Kibana dashboard

