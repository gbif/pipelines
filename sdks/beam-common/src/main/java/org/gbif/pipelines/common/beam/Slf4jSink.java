package org.gbif.pipelines.common.beam;

import com.codahale.metrics.MetricRegistry;
import java.util.Properties;
import org.apache.beam.runners.spark.metrics.WithMetricsSupport;

/**
 * A Spark {@link org.apache.spark.metrics.sink.Sink} that is tailored to report metrics to Slf4j.
 */
public class Slf4jSink extends org.apache.spark.metrics.sink.Slf4jSink {

  public Slf4jSink(final Properties properties, final MetricRegistry metricRegistry) {
    super(properties, WithMetricsSupport.forRegistry(metricRegistry));
  }
}
