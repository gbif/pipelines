package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.IMAGE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
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
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Image extension, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form{@link ExtendedRecord} to {@link ImageRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link ImageRecord} using {@link ExtendedRecord} as
 * a source and {@link ImageInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/images.xml</a>
 */
public class ImageTransform extends Transform<ExtendedRecord, ImageRecord> {

  private final SerializableFunction<String, String> preprocessDateFn;
  private final List<DateComponentOrdering> orderings;
  private ImageInterpreter imageInterpreter;

  @Builder(buildMethodName = "create")
  private ImageTransform(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    super(ImageRecord.class, IMAGE, ImageTransform.class.getName(), IMAGE_RECORDS_COUNT);
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (imageInterpreter == null) {
      imageInterpreter =
          ImageInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public ImageTransform init() {
    setup();
    return this;
  }

  /** Maps {@link ImageRecord} to key value, where key is {@link ImageRecord#getId} */
  public MapElements<ImageRecord, KV<String, ImageRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ImageRecord>>() {})
        .via((ImageRecord ir) -> KV.of(ir.getId(), ir));
  }

  public ImageTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<ImageRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                ImageRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.IMAGE))
        .via(imageInterpreter::interpret)
        .getOfNullable();
  }
}
