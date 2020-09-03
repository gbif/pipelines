package org.gbif.pipelines.ingest.java.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.beam.runners.core.metrics.DefaultMetricResults;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;

/**
 * Metrics to support {@link org.gbif.pipelines.transforms.Transform} counters, wrapper on top of
 * {@link org.apache.beam.sdk.metrics.MetricResults}
 */
@AllArgsConstructor(staticName = "create")
public class IngestMetrics {

  private final Map<String, String> nameSpaceMap = new HashMap<>();
  private final Map<String, AtomicLong> valueMap = new HashMap<>();

  public IngestMetrics addMetric(Class<?> namespace, String name) {
    return addMetric(namespace.getName(), name);
  }

  public IngestMetrics addMetric(String namespace, String name) {
    valueMap.putIfAbsent(name, new AtomicLong(0L));
    nameSpaceMap.putIfAbsent(name, namespace);
    return this;
  }

  public long incMetric(String name) {
    return Optional.ofNullable(valueMap.get(name)).map(AtomicLong::incrementAndGet).orElse(0L);
  }

  public MetricResults getMetricsResult() {
    List<MetricResult<Long>> counters =
        valueMap.entrySet().stream()
            .filter(x -> x.getValue().get() > 0)
            .map(
                s -> {
                  MetricKey metricKey =
                      MetricKey.create(
                          null, MetricName.named(nameSpaceMap.get(s.getKey()), s.getKey()));
                  return MetricResult.create(metricKey, false, s.getValue().longValue());
                })
            .collect(Collectors.toList());

    return new DefaultMetricResults(counters, Collections.emptyList(), Collections.emptyList());
  }
}
