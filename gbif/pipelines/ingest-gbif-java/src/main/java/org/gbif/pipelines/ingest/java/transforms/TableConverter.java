package org.gbif.pipelines.ingest.java.transforms;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;

@Builder
public class TableConverter<T extends SpecificRecordBase> {

  private final IngestMetrics metrics;

  @NonNull private final String counterName;

  @NonNull private final Map<String, ExtendedRecord> verbatimMap;

  @NonNull
  private final SerializableBiFunction<GbifIdRecord, ExtendedRecord, Optional<T>> converterFn;

  /** Join all records, convert into OccurrenceHdfsRecord and save as an avro file */
  public Function<GbifIdRecord, Optional<T>> getFn() {
    return id -> {
      String k = id.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());

      Optional<T> table = converterFn.apply(id, er);

      table.ifPresent(x -> metrics.incMetric(counterName));

      return table;
    };
  }
}
