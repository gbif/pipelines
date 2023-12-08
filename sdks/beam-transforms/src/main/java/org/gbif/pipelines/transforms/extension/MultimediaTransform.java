package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValueNullAware;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
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

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private MultimediaInterpreter multimediaInterpreter;

  @Builder(buildMethodName = "create")
  private MultimediaTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(
        MultimediaRecord.class,
        MULTIMEDIA,
        MultimediaTransform.class.getName(),
        MULTIMEDIA_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (multimediaInterpreter == null) {
      multimediaInterpreter =
          MultimediaInterpreter.builder()
              .ordering(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public MultimediaTransform init() {
    setup();
    return this;
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
                hasExtension(er, Extension.MULTIMEDIA)
                    || hasValueNullAware(er, DwcTerm.associatedMedia))
        .via(multimediaInterpreter::interpret)
        .via(MultimediaInterpreter::interpretAssociatedMedia)
        .getOfNullable();
  }
}
