package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MULTIMEDIA;

import java.time.Instant;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Multimedia extension, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form{@link ExtendedRecord} to {@link MultimediaRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MultimediaRecord} using {@link
 * ExtendedRecord} as a source and {@link MultimediaInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/multimedia.xml</a>
 */
public class MultimediaTransform extends Transform<ExtendedRecord, MultimediaRecord> {

  public MultimediaTransform() {
    super(
        MultimediaRecord.class,
        MULTIMEDIA,
        MultimediaTransform.class.getName(),
        MULTIMEDIA_RECORDS_COUNT);
  }

  public static MultimediaTransform create() {
    return new MultimediaTransform();
  }

  /** Maps {@link MultimediaRecord} to key value, where key is {@link MultimediaRecord#getId} */
  public MapElements<MultimediaRecord, KV<String, MultimediaRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
        .via((MultimediaRecord mr) -> KV.of(mr.getId(), mr));
  }

  public MultimediaTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<MultimediaRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                MultimediaRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(
            er ->
                Optional.ofNullable(er.getExtensions().get(Extension.MULTIMEDIA.getRowType()))
                        .filter(l -> !l.isEmpty())
                        .isPresent()
                    || ModelUtils.extractOptValue(er, DwcTerm.associatedMedia).isPresent())
        .via(MultimediaInterpreter::interpret)
        .via(MultimediaInterpreter::interpretAssociatedMedia)
        .getOfNullable();
  }
}
