# The example demonstrates how to send Apache Beam SparkRunner metrics to [ELK](https://www.elastic.co/elk-stack) and use result for Kibana dashboards

## Main objective

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

#### 2.2 Spark log4j configuration

### 3. Logstash configuration and templates

#### 3.1 Logstash main configuration

#### 3.2 Logging index template

### 4. Create Kibana dashboard

