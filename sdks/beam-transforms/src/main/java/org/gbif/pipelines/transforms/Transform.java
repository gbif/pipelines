package org.gbif.pipelines.transforms;

import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.transforms.common.CheckTransforms;

/**
 * Common class for all transformations
 *
 * <p>Beam level transformations for the Amplification extension, reads an avro, writes an avro,
 * maps from value to keyValue and transforms form {@link R} to {@link T}.
 */
public abstract class Transform<R, T extends SpecificRecordBase & Record> extends DoFn<R, T> {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private final TupleTag<T> tag = new TupleTag<T>() {};
  private final TupleTag<Edge<T>> edgeTag = new TupleTag<Edge<T>>() {};
  private final InterpretationType recordType;
  private final String baseName;
  private final String baseInvalidName;
  private final Class<T> clazz;
  private final String counterName;

  private Counter counter;
  private SerializableConsumer<String> counterFn = v -> counter.inc();

  public Transform(
      Class<T> clazz, InterpretationType recordType, String counterNamespace, String counterName) {
    this.clazz = clazz;
    this.recordType = recordType;
    this.baseName = recordType.name().toLowerCase();
    this.baseInvalidName = baseName + "_invalid";
    this.counterName = counterName;
    this.counter = Metrics.counter(counterNamespace, counterName);
  }

  public void setCounterFn(SerializableConsumer<String> counterFn) {
    this.counterFn = counterFn;
  }

  protected InterpretationType getRecordType() {
    return recordType;
  }

  /**
   * Default {@link #check(Set, Class)} that returns a {@link CheckTransforms} of {@link
   * ExtendedRecord}.
   */
  public CheckTransforms<ExtendedRecord> check(Set<String> types) {
    return check(types, ExtendedRecord.class);
  }

  /**
   * Checks if list contains {@link InterpretationType}, else returns empty {@link PCollection<T>}.
   *
   * <p>This method should be used only when the default {@link #check(Set)} doesn't fill the needs.
   */
  public <S> CheckTransforms<S> check(Set<String> types, Class<S> outputClass) {
    return CheckTransforms.create(outputClass, CheckTransforms.checkRecordType(types, recordType));
  }

  /** Useful for the java version of pipelines where you only want to use boolean */
  public boolean checkType(Set<String> types) {
    return CheckTransforms.checkRecordType(types, recordType);
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
   * @param path path to source files
   */
  public AvroIO.Read<T> read(String path, boolean acceptEmpty) {
    if (acceptEmpty) {
      return AvroIO.read(clazz).from(path).withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW);
    }
    return AvroIO.read(clazz).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link T}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link
   *     Transform#baseName}
   */
  public AvroIO.Read<T> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(baseName));
  }

  /**
   * Reads avro files from path, which contains {@link T}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link
   *     Transform#baseName}
   */
  public AvroIO.Read<T> read(UnaryOperator<String> pathFn, boolean acceptEmpty) {
    if (acceptEmpty) {
      return read(pathFn.apply(baseName), acceptEmpty);
    }
    return read(pathFn.apply(baseName));
  }

  /**
   * Writes {@link T} *.avro files to path, data will be split into several files, uses Snappy
   * compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<T> write(String toPath) {
    return AvroIO.write(clazz).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link T} *.avro files to path, data will be split into several files, uses Snappy
   * compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link
   *     Transform#baseName}
   */
  public AvroIO.Write<T> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(baseName));
  }

  /**
   * Writes {@link T} *.avro files to path, data will be split into several files, uses Snappy
   * compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link
   *     Transform#baseInvalidName}
   */
  public AvroIO.Write<T> writeInvalid(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(baseInvalidName));
  }

  /** Creates an {@link R} for {@link T} */
  public SingleOutput<R, T> interpret() {
    return ParDo.of(this);
  }

  public String getBaseName() {
    return baseName;
  }

  public String getBaseInvalidName() {
    return baseInvalidName;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element()).ifPresent(c::output);
  }

  public Optional<T> processElement(R source) {
    Optional<T> convert = convert(source);
    convert.ifPresent(t -> incCounter());
    return convert;
  }

  public abstract Optional<T> convert(R source);

  public void incCounter() {
    counterFn.accept(counterName);
  }

  /** @return TupleTag required for grouping */
  public TupleTag<T> getTag() {
    return tag;
  }

  public TupleTag<Edge<T>> getEdgeTag() {
    return edgeTag;
  }

  public Class<T> getReturnClazz() {
    return clazz;
  }

  @SneakyThrows
  public Schema getAvroSchema() {
    return clazz.newInstance().getSchema();
  }

  public static CodecFactory getBaseCodec() {
    return BASE_CODEC;
  }
}
