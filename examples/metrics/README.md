# The example demonstrates how to send Apache Beam SparkRunner metrics to ELK and use the result for Kibana dashboards

## Main objective
[ELK](https://www.elastic.co/elk-stack)

### 1. Add SLF4J realisation for Spark

#### 1.1 Depencies:

#### 1.2 Slf4J class reaslisation:

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

### 2. Provide Spark metrics and logger configurations

#### 2.1 Spark metrics configuration
[metrics.properties](./src/resources/metrics.properties)
```
```

#### 2.2 Spark log4j configuration
[log4j.properties](./src/resources/log4j.properties)
```
```

### 3. Logstash main configuration

```
input {
    gelf {
        host=>"127.0.0.1"
        port=>"12201"
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
        codec=>"rubydebug"
    }
    elasticsearch {
        hosts=>"localhost:9200"
        index=>"examples-metrics-%{+YYYY.MM.dd}"
    }
}
```

### 4. How to run the examaple


```
elasticsearch/bin/elasticsearch
```

```
kibana/bin/kibana
```

```
logstash/bin/logstash -f examples-metrics.config
```

```
java -jar target/examples-metrics-BUILD_VERSION-shaded.jar src/main/resources/example.properties
```

### 5. Change logging template index
[Logstash mapping](https://www.elastic.co/blog/logstash_lesson_elasticsearch_mapping)

### 6. Create Kibana dashboard

