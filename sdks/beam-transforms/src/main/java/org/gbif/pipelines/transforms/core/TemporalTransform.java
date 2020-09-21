package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TEMPORAL;

import java.time.Instant;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
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

  private TemporalInterpreter temporalInterpreter;

  private TemporalTransform() {
    super(
        TemporalRecord.class, TEMPORAL, TemporalTransform.class.getName(), TEMPORAL_RECORDS_COUNT);
  }

  public static TemporalTransform create() {
    TemporalTransform tr = new TemporalTransform();
    tr.temporalInterpreter = TemporalInterpreter.getInstance();
    return tr;
  }

  /**
   * Support extra date formats
   *
   * @param dateComponentOrdering
   * @return
   */
  public static TemporalTransform create(DateComponentOrdering[] dateComponentOrdering) {
    TemporalTransform tr = new TemporalTransform();
    if (dateComponentOrdering != null) {
      tr.temporalInterpreter = TemporalInterpreter.getInstance(dateComponentOrdering);
    } else {
      tr.temporalInterpreter = TemporalInterpreter.getInstance();
    }
    return tr;
  }

  /** Maps {@link TemporalRecord} to key value, where key is {@link TemporalRecord#getId} */
  public MapElements<TemporalRecord, KV<String, TemporalRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
        .via((TemporalRecord tr) -> KV.of(tr.getId(), tr));
  }

  public TemporalTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<TemporalRecord> convert(ExtendedRecord source) {
    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    return Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(temporalInterpreter::interpretTemporal)
        .via(temporalInterpreter::interpretModified)
        .via(temporalInterpreter::interpretDateIdentified)
        .getOfNullable();
  }
}
