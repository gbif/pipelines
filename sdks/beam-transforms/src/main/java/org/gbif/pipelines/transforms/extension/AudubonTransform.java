package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUDUBON;

import java.time.Instant;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
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

  private AudubonTransform() {
    super(AudubonRecord.class, AUDUBON, AudubonTransform.class.getName(), AUDUBON_RECORDS_COUNT);
  }

  public static AudubonTransform create() {
    return new AudubonTransform();
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
        .when(
            er ->
                Optional.ofNullable(er.getExtensions().get(Extension.AUDUBON.getRowType()))
                    .filter(l -> !l.isEmpty())
                    .isPresent())
        .via(AudubonInterpreter::interpret)
        .getOfNullable();
  }
}
