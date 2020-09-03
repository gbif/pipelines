package org.gbif.pipelines.ingest.java.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.metrics.MetricResults;
import org.junit.Assert;
import org.junit.Test;

public class IngestMetricsTest {

  @Test
  public void addStringMetricsTest() {

    // State
    Class<IngestMetricsTest> namespace = IngestMetricsTest.class;
    String name = "someName";

    // When
    IngestMetrics metrics = IngestMetrics.create().addMetric(namespace, name);
    metrics.incMetric(name);
    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(name));
  }

  @Test
  public void addClassMetricsTest() {

    // State
    String namespace = IngestMetricsTest.class.getName();
    String name = "someName";

    // When
    IngestMetrics metrics = IngestMetrics.create().addMetric(namespace, name);
    metrics.incMetric(name);
    metrics.incMetric(name);
    metrics.incMetric(name);
    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(3L), map.get(name));
  }

  @Test
  public void emptyNameMetricsTest() {

    // State
    String name = "someName";

    // When
    IngestMetrics metrics = IngestMetrics.create();
    metrics.incMetric(name);
    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(0, map.size());
    Assert.assertNull(map.get(name));
  }

  @Test
  public void mixMetricsTest() {

    // State
    Class<IngestMetricsTest> namespace = IngestMetricsTest.class;
    String name = "someName";
    String namespace2 = IngestMetricsTest.class.getName() + "2";
    String name2 = "someName2";

    // When
    IngestMetrics metrics =
        IngestMetrics.create().addMetric(namespace, name).addMetric(namespace2, name2);
    metrics.incMetric(name);
    metrics.incMetric(name);
    metrics.incMetric(name);
    metrics.incMetric(name2);
    metrics.incMetric(name2);
    metrics.incMetric(name2);
    metrics.incMetric(name2);
    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(Long.valueOf(3L), map.get(name));
    Assert.assertEquals(Long.valueOf(4L), map.get(name2));
  }

  @Test
  public void concurrentMetricsTest() throws Exception {

    // State
    Class<IngestMetricsTest> namespace = IngestMetricsTest.class;
    String name = "someName";
    String namespace2 = IngestMetricsTest.class.getName() + "2";
    String name2 = "someName2";
    Long count = 10L;

    ExecutorService executor = Executors.newWorkStealingPool(2);

    // When
    IngestMetrics metrics =
        IngestMetrics.create().addMetric(namespace, name).addMetric(namespace2, name2);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int x = 0; x < count; x++) {
      futures.add(CompletableFuture.runAsync(() -> metrics.incMetric(name), executor));
      futures.add(CompletableFuture.runAsync(() -> metrics.incMetric(name2), executor));
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    MetricResults result = metrics.getMetricsResult();

    executor.shutdown();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(count, map.get(name));
    Assert.assertEquals(count, map.get(name2));
  }
}
