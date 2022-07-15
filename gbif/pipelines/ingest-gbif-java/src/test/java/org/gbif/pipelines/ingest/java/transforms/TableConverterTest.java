package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.junit.Assert;
import org.junit.Test;

public class TableConverterTest {

  private static final String ID = "777";

  @Test
  public void converterTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToHdfsViewMetrics();
    GbifIdRecord idRecord = GbifIdRecord.newBuilder().setId(ID).setGbifId(1L).build();

    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId(ID).setExtensions(ext).build();

    // When
    Optional<MeasurementOrFactTable> measurementOrFactTable =
        TableConverter.<MeasurementOrFactTable>builder()
            .metrics(metrics)
            .verbatimMap(Collections.singletonMap(ID, extendedRecord))
            .converterFn(MeasurementOrFactTableConverter::convert)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .build()
            .getFn()
            .apply(idRecord);

    // Should
    Assert.assertNotNull(measurementOrFactTable);
    Assert.assertTrue(measurementOrFactTable.isPresent());
    Assert.assertEquals("1", measurementOrFactTable.get().getGbifid());

    Map<String, Long> map = new HashMap<>();
    metrics
        .getMetricsResult()
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT));
  }
}
