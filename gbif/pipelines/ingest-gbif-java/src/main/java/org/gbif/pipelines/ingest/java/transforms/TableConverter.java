package org.gbif.pipelines.ingest.java.transforms;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.pojo.ErIdrMdrContainer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

@Builder
public class TableConverter<T extends SpecificRecordBase> {

  private final IngestMetrics metrics;

  @NonNull private final String counterName;

  @NonNull private final MetadataRecord metadataRecord;

  @NonNull private final Map<String, ExtendedRecord> verbatimMap;

  @NonNull private final SerializableFunction<ErIdrMdrContainer, List<T>> converterFn;

  /** Join all records, convert into OccurrenceHdfsRecord and save as an avro file */
  public Function<IdentifierRecord, List<T>> getFn() {
    return id -> {
      String k = id.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());

      List<T> table = converterFn.apply(ErIdrMdrContainer.create(er, id, metadataRecord));

      table.forEach(x -> metrics.incMetric(counterName));

      return table;
    };
  }
}
