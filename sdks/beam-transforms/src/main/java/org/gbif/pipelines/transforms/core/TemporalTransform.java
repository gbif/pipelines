package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TEMPORAL;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Event, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link TemporalRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
 * as a source and {@link TemporalInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#event">https://dwc.tdwg.org/terms/#event</a>
 */
public class TemporalTransform extends Transform<ExtendedRecord, TemporalRecord> {

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private TemporalInterpreter temporalInterpreter;

  @Builder(buildMethodName = "create")
  private TemporalTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        TemporalRecord.class, TEMPORAL, TemporalTransform.class.getName(), TEMPORAL_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (temporalInterpreter == null) {
      temporalInterpreter =
          TemporalInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getCoreId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> asKv(boolean useCoreId) {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(useCoreId ? tr.getCoreId() : tr.getId(), tr));
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> toKv() {
    return asKv(false);
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getCoreId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> toCoreIdKv() {
    return asKv(true);
  }

  public TemporalTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup can be applied only to void method */
  public TemporalTransform init() {
    setup();
    return this;
  }

  @Override
  public Optional<TemporalRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                TemporalRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(temporalInterpreter::interpretTemporal)
        .via(temporalInterpreter::interpretModified)
        .via(temporalInterpreter::interpretDateIdentified)
        .via(TemporalInterpreter::setCoreId)
        .via(TemporalInterpreter::setParentEventId)
        .getOfNullable();
  }
}
