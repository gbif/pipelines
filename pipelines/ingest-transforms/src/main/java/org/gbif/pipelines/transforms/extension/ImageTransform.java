package org.gbif.pipelines.transforms.extension;

import java.util.List;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.transforms.CheckTransforms;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IMAGE;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Image extension, read an avro, write an avro, from value to keyValue and
 * transforms form{@link ExtendedRecord} to {@link ImageRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/images.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ImageTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  /**
   * Checks if list contains {@link RecordType#IMAGE}, else returns empty {@link PCollection <ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, IMAGE));
  }

  /** Maps {@link ImageRecord} to key value, where key is {@link ImageRecord#getId} */
  public static MapElements<ImageRecord, KV<String, ImageRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ImageRecord>>() {})
        .via((ImageRecord ir) -> KV.of(ir.getId(), ir));
  }

  /**
   * Reads avro files from path, which contains {@link ImageRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<ImageRecord> read(String path) {
    return AvroIO.read(ImageRecord.class).from(path);
  }

  /**
   * Writes {@link ImageRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<ImageRecord> write(String toPath) {
    return AvroIO.write(ImageRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * ParDo runs sequence of interpretations for {@link ImageRecord} using {@link
   * ExtendedRecord} as a source and {@link ImageInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, ImageRecord> {

    private final Counter counter = Metrics.counter(ImageTransform.class, IMAGE_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> ImageRecord.newBuilder().setId(er.getId()).build())
          .when(er -> Optional.ofNullable(er.getExtensions().get(Extension.IMAGE.getRowType()))
              .filter(l -> !l.isEmpty())
              .isPresent())
          .via(ImageInterpreter::interpret)
          .consume(context::output);

      counter.inc();
    }
  }
}
