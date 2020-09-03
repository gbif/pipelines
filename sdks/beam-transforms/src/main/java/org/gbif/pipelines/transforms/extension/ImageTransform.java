package org.gbif.pipelines.transforms.extension;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IMAGE;

import java.time.Instant;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
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

  private ImageTransform() {
    super(ImageRecord.class, IMAGE, ImageTransform.class.getName(), IMAGE_RECORDS_COUNT);
  }

  public static ImageTransform create() {
    return new ImageTransform();
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
        .when(
            er ->
                Optional.ofNullable(er.getExtensions().get(Extension.IMAGE.getRowType()))
                    .filter(l -> !l.isEmpty())
                    .isPresent())
        .via(ImageInterpreter::interpret)
        .getOfNullable();
  }
}
