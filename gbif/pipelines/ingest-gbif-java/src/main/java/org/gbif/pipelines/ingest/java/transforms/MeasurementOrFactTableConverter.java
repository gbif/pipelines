package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactTable;

@Builder
public class MeasurementOrFactTableConverter {

  private final IngestMetrics metrics;

  @NonNull private final Map<String, ExtendedRecord> verbatimMap;

  /** Join all records, convert into OccurrenceHdfsRecord and save as an avro file */
  public Function<BasicRecord, Optional<MeasurementOrFactTable>> getFn() {
    return br -> {
      String k = br.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());

      Optional<MeasurementOrFactTable> table =
          org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter.convert(br, er);

      table.ifPresent(x -> metrics.incMetric(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT));

      return table;
    };
  }
}
