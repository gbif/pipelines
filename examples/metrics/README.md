# The example demonstrates how to create and send Apache Beam SparkRunner metrics to ELK and use the result for Kibana dashboards

## Main objective
[ELK](https://www.elastic.co/elk-stack)

### Base sequence of actions to get the result:
 - 1 [Java project level:](#java-project-level)
    - 1.1 [Add all necessary dependencies to the project](#dependencies)
    - 1.2 [Create Apache Beam pipeline class](#)
    - 1.3 [Add ParDo function with Apache Beam to the class](#)
    - 1.4 [Add additional information to the logger](#)
    - 1.5 [Create Spark Slf4j adapter for Apache Beam](#)
 - 2 [Spark level:](#spark-level)
    - 2.1 [Create metrics properties](#)
    - 2.2 [Create log4j properties](#)
 - 3 [Logstash level:](#)
    - 3.1 [Create Logstash configuration](#create-logstash-configuration)
 - 4 [How to run:](#how-to-run)
    - 4.1 [Run ELK](#run-elk)
    - 4.2 [Run Spark standalone](#run-spark-standalone)
    - 4.3 [Run Spark cluster](#run-spark-cluster)
 - 5 [Elasticsearch level:](#)
    - 5.1 [Change index template](#)
 - 6 [Kibana level:](#)
    - 6.1 [Update index patterns](#)
    - 6.2 [Create visualization and dashboard](#)

---
### 1. Java project level

#### 1.1 Dependencies
Please check [pom.xml](./pom.xml) to find out dependencies and plugins, remember if you use Spark cluster, you must delete Spark section

[](http://logging.paluch.biz/)

#### 1.2 Create a pipeline class

Read more about [Apache Beam pipelines](https://beam.apache.org/get-started/wordcount-example/)

Example[1]:

```java
package org.gbif.pipelines.examples;

import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;

import java.util.UUID;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MetricsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsPipeline.class);

  public static void main(String[] args) {

    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, args);
    Pipeline p = Pipeline.create(options);

    p.apply("Reads file", TextIO.read().from(options.getInputPath()))
        .apply("Filters words", ParDo.of(new FilterTextFn()));

    LOG.info("Running the pipeline");
    p.run().waitUntilFinish();
    LOG.info("Pipeline has been finished");
  }
}
```

#### 1.3 Create DoFn with Apache Beam Counter

Read more about [Apache Beam Metrics](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/metrics/Metrics.html)

Example[1]:

```java
  /**
   * Simple DoFn filters non-foo words. Using {@link Counter} we can find out how many records were
   * filtered
   */
  private static class FilterTextFn extends DoFn<String, String> {

    private final Counter fooCounter = Metrics.counter(MetricsPipeline.class, "foo");
    private final Counter nonfooCounter = Metrics.counter(MetricsPipeline.class, "nonfoo");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String element = c.element();
      if (element.equals("foo")) {
        fooCounter.inc();
        c.output(element);
      } else {
        nonfooCounter.inc();
      }
    }
  }
```

#### 1.4 Add an additional information to the logger

Call MDC put information after main method

Example[1]:

```java
MDC.put("uuid", UUID.randomUUID().toString());
```

#### 1.5 Slf4J class realisation

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

[1] Find the final example [MetricsPipeline](./src/org/gbif/pipelines/examples/MetricsPipeline) class.

---

### 2 Spark level

#### 2.1 Spark metrics configuration file

Create metrics.properties file, necessary for Spark monitoring, please read about [Spark metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics)

```properties
executor.sink.slf4j.class=org.apache.spark.metrics.sink.Slf4jSink
driver.sink.slf4j.class=org.gbif.pipelines.common.beam.Slf4jSink
```
Find the final example [metrics.properties](./src/main/resources/metrics.properties) file.

#### 2.2 Spark log4j configuration file

Add ELK appender part to Spark log4j properties, please read about [Logstash/Gelf Loggers](http://logging.paluch.biz/examples/log4j-1.2.x.html)

```properties
# ELK appender
log4j.appender.gelf=biz.paluch.logging.gelf.log4j.GelfLogAppender
log4j.appender.gelf.Threshold=INFO
log4j.appender.gelf.Host=udp:127.0.0.1
log4j.appender.gelf.Port=12201
log4j.appender.gelf.Version=1.1
log4j.appender.gelf.Facility=examples-metrics
log4j.appender.gelf.ExtractStackTrace=true
log4j.appender.gelf.FilterStackTrace=true
log4j.appender.gelf.MdcProfiling=true
log4j.appender.gelf.TimestampPattern=yyyy-MM-dd HH:mm:ss,SSSS
log4j.appender.gelf.MaximumMessageSize=8192
log4j.appender.gelf.MdcFields=uuid
log4j.appender.gelf.IncludeFullMdc=true
```
Find the final example [log4j.properties](./src/main/resources/log4j.properties) file.
---

### 3 Logstash level

#### 3.1 Create Logstash configuration

Create a simple Logstash configuration file and call it `examples-metrics.config`. For more detailed information please read articles [Logstash configuration file structure](https://www.elastic.co/guide/en/logstash/current/configuration-file-structure.html) and [more complex examples](https://www.elastic.co/guide/en/logstash/current/config-examples.html)

#### Input section
Add input section to listen the host and port for [GELF(The Graylog Extended Log Format)](http://docs.graylog.org/en/2.4/pages/gelf.html) messages
```
input {
    gelf {
        host => "127.0.0.1"
        port => "12201"
    }
}
```

#### Filter section
In our case necessary information about metrics is stored in `message` field.
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

- `source` - a field for parsing
- `target` - a new field for parsed result
- `field_split` - split characters, in our case it is a comma
- `trim_key` - to remove spaces in a key
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

#### Output section
For output logs to console and Elasticsearch add output section:
```
output {
    stdout {
        codec => "rubydebug"
    }
    elasticsearch {
        hosts => "localhost:9200"
        index => "examples-metrics"
    }
}
```

#### Completed configuration:
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
        index => "examples-metrics"
    }
}
```
---

### 4. How to run

#### 4.1 Run Spark standalone
If you don't have an [ELK](https://www.elastic.co/elk-stack) instance, you can:

[Download and run Elasticsearch](https://www.elastic.co/downloads/elasticsearch)
```shell
elasticsearch/bin/elasticsearch
```

[Download and run Kibana](https://www.elastic.co/downloads/kibana)
```shell
kibana/bin/kibana
```

[Download and run Logstash](https://www.elastic.co/downloads/logstash) using `examples-metrics.config` configuration created in step [3](#3-logstash-main-configuration)
```shell
logstash/bin/logstash -f examples-metrics.config
```

#### 4.2 Run Spark cluster

Standalone Spark, [build the project](https://github.com/gbif/pipelines#how-to-build-the-project) and run:
```shell
java -jar target/examples-metrics-BUILD_VERSION-shaded.jar src/main/resources/example.properties
```

Or run in a Spark cluster:
- Remove Spark section in the project [pom.xml](./pom.xml)
- Download [logstash-gelf-1.11.2.jar](http://central.maven.org/maven2/biz/paluch/logging/logstash-gelf/1.12.0/logstash-gelf-1.12.0.jar) library
- [Build the project](https://github.com/gbif/pipelines#how-to-build-the-project)
- Copy examples-metrics-BUILD_VERSION-shaded.jar, your configs and properties to your Spark gateway and run
```shell
spark2-submit --conf spark.metrics.conf=metrics.properties --conf "spark.driver.extraClassPath=logstash-gelf-1.11.2.jar" --driver-java-options "-Dlog4j.configuration=file:log4j.properties" --class org.gbif.pipelines.examples.MetricsPipeline --master yarn examples-metrics-BUILD_VERSION-shaded.jar --runner=SparkRunner --inputPath=foobar.txt
```
---

### 5. Change logging index template
[Logstash mapping](https://www.elastic.co/blog/logstash_lesson_elasticsearch_mapping)

---
### 6. Create Kibana dashboard

