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
