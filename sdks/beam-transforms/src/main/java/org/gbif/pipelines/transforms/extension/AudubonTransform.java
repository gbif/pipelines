package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.AUDUBON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Audubon extension, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form {@link ExtendedRecord} to {@link AudubonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link AudubonRecord} using {@link ExtendedRecord}
 * as a source and {@link AudubonInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/ac/audubon.xml</a>
 */
public class AudubonTransform extends Transform<ExtendedRecord, AudubonRecord> {

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private AudubonInterpreter audubonInterpreter;

  @Builder(buildMethodName = "create")
  private AudubonTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(AudubonRecord.class, AUDUBON, AudubonTransform.class.getName(), AUDUBON_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (audubonInterpreter == null) {
      audubonInterpreter =
          AudubonInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public AudubonTransform init() {
    setup();
    return this;
  }

  /** Maps {@link AudubonRecord} to key value, where key is {@link AudubonRecord#getId} */
  public MapElements<AudubonRecord, KV<String, AudubonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AudubonRecord>>() {})
        .via((AudubonRecord ar) -> KV.of(ar.getId(), ar));
  }

  public AudubonTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<AudubonRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                AudubonRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.AUDUBON))
        .via(audubonInterpreter::interpret)
        .getOfNullable();
  }
}
