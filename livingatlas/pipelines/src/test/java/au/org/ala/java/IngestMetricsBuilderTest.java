package au.org.ala.java;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.*;

import au.org.ala.pipelines.java.IngestMetricsBuilder;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.MetricResults;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.junit.Assert;
import org.junit.Test;

public class IngestMetricsBuilderTest {

  @Test
  public void createVerbatimToInterpretedMetricsTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createVerbatimToInterpretedMetrics();

    // When
    metrics.incMetric(BASIC_RECORDS_COUNT);
    metrics.incMetric(LOCATION_RECORDS_COUNT);
    metrics.incMetric(METADATA_RECORDS_COUNT);
    metrics.incMetric(TAXON_RECORDS_COUNT);
    metrics.incMetric(TEMPORAL_RECORDS_COUNT);
    metrics.incMetric(VERBATIM_RECORDS_COUNT);
    metrics.incMetric(AUDUBON_RECORDS_COUNT);
    metrics.incMetric(IMAGE_RECORDS_COUNT);
    metrics.incMetric(MEASUREMENT_OR_FACT_RECORDS_COUNT);
    metrics.incMetric(MULTIMEDIA_RECORDS_COUNT);
    metrics.incMetric(FILTER_ER_BASED_ON_GBIF_ID);
    metrics.incMetric(UNIQUE_GBIF_IDS_COUNT);
    metrics.incMetric(DUPLICATE_GBIF_IDS_COUNT);
    metrics.incMetric(IDENTICAL_GBIF_OBJECTS_COUNT);
    metrics.incMetric(INVALID_GBIF_ID_COUNT);
    metrics.incMetric(UNIQUE_IDS_COUNT);
    metrics.incMetric(DUPLICATE_IDS_COUNT);
    metrics.incMetric(IDENTICAL_OBJECTS_COUNT);

    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(18, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(BASIC_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(LOCATION_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(METADATA_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(TAXON_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(TEMPORAL_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(VERBATIM_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(AUDUBON_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(IMAGE_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(MEASUREMENT_OR_FACT_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(MULTIMEDIA_RECORDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(FILTER_ER_BASED_ON_GBIF_ID));
    Assert.assertEquals(Long.valueOf(1L), map.get(UNIQUE_GBIF_IDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(DUPLICATE_GBIF_IDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(IDENTICAL_GBIF_OBJECTS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(INVALID_GBIF_ID_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(UNIQUE_IDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(DUPLICATE_IDS_COUNT));
    Assert.assertEquals(Long.valueOf(1L), map.get(IDENTICAL_OBJECTS_COUNT));
  }

  @Test
  public void createInterpretedToEsIndexMetricsTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();

    // When
    metrics.incMetric(AVRO_TO_JSON_COUNT);

    MetricResults result = metrics.getMetricsResult();

    // Should
    Map<String, Long> map = new HashMap<>();
    result
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(AVRO_TO_JSON_COUNT));
  }
}
