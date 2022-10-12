package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.junit.Assert;
import org.junit.Test;

public class TableConverterTest {

  private static final String ID = "777";

  @Test
  public void converterTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToHdfsViewMetrics();
    IdentifierRecord idRecord = IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder().setId(ID).setDatasetKey("dataset_key").build();

    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId(ID).setExtensions(ext).build();

    // When
    List<MeasurementOrFactTable> measurementOrFactTable =
        TableConverter.<MeasurementOrFactTable>builder()
            .metrics(metrics)
            .metadataRecord(metadataRecord)
            .verbatimMap(Collections.singletonMap(ID, extendedRecord))
            .converterFn(MeasurementOrFactTableConverter::convert)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .build()
            .getFn()
            .apply(idRecord);

    // Should
    Assert.assertNotNull(measurementOrFactTable);
    Assert.assertFalse(measurementOrFactTable.isEmpty());
    Assert.assertEquals("1", measurementOrFactTable.get(0).getGbifid());

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
