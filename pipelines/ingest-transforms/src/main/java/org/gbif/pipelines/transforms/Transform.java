package org.gbif.pipelines.transforms;

import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PCollection;


/**
 * Common class for all transformations
 * <p>
 * Beam level transformations for the Amplification extension, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link R} to {@link T}.
 */
public abstract class Transform<R, T extends SpecificRecordBase> extends DoFn<R, T> {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private final RecordType recordType;
  private final String baseName;
  private final Class<T> clazz;

  public Transform(Class<T> clazz, RecordType recordType) {
    this.clazz = clazz;
    this.recordType = recordType;
    this.baseName = recordType.name().toLowerCase();
  }

  protected RecordType getRecordType() {
    return recordType;
  }

  /**
   * Checks if list contains {@link RecordType#BASIC}, else returns empty {@link PCollection<T>}
   */
  public CheckTransforms<ExtendedRecord> check(Set<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, CheckTransforms.checkRecordType(types, recordType));
  }

  /**
   * Reads avro files from path, which contains {@link T}
   *
   * @param path path to source files
   */
  public AvroIO.Read<T> read(String path) {
    return AvroIO.read(clazz).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link T}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link Transform#baseName}
   */
  public AvroIO.Read<T> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(baseName));
  }

  /**
   * Writes {@link T} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<T> write(String toPath) {
    return AvroIO.write(clazz).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link T} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link Transform#baseName}
   */
  public AvroIO.Write<T> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(baseName));
  }

  /**
   * Creates an {@link BasicTransform} for {@link BasicRecord}
   */
  public SingleOutput<R, T> interpret() {
    return ParDo.of(this);
  }

  public String getBaseName() {
    return baseName;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element()).ifPresent(r -> {
      c.output(r);
      incCounter();
    });
  }

  public abstract Optional<T> processElement(R source);

  public abstract void incCounter();

}
