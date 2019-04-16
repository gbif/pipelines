package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.util.List;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.config.KeygenConfigFactory;
import org.gbif.pipelines.transforms.CheckTransforms;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.joda.time.DateTime;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link BasicRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BasicTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = BASIC.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#BASIC}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, BASIC));
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public static MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /**
   * Reads avro files from path, which contains {@link BasicRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<BasicRecord> read(String path) {
    return AvroIO.read(BasicRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link BasicRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link BasicTransform#BASE_NAME}
   */
  public static AvroIO.Read<BasicRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link BasicRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<BasicRecord> write(String toPath) {
    return AvroIO.write(BasicRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link BasicRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link BasicTransform#BASE_NAME}
   */
  public static AvroIO.Write<BasicRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link BasicRecord}
   */
  public static SingleOutput<ExtendedRecord, BasicRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * Creates an {@link Interpreter} for {@link BasicRecord}
   */
  public static SingleOutput<ExtendedRecord, BasicRecord> interpret(String propertiesPath, String datasetId) {
    return ParDo.of(new Interpreter(propertiesPath, datasetId));
  }

  /**
   * ParDo runs sequence of interpretations for {@link BasicRecord} using {@link ExtendedRecord} as
   * a source and {@link BasicInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, BasicRecord> {

    private final Counter counter = Metrics.counter(BasicTransform.class, BASIC_RECORDS_COUNT);

    private final KeygenConfig keygenConfig;
    private final String datasetId;
    private Connection connection;
    private HBaseLockingKeyService keygenService;

    public Interpreter(String propertiesPath, String datasetId) {
      this.keygenConfig = KeygenConfigFactory.create(Paths.get(propertiesPath));
      this.datasetId = datasetId;
    }

    public Interpreter(KeygenConfig keygenConfig, String datasetId) {
      this.keygenConfig = keygenConfig;
      this.datasetId = datasetId;
    }


    public Interpreter() {
      this.keygenConfig = null;
      this.datasetId = null;
    }

    @SneakyThrows
    @Setup
    public void setup() {
      if (keygenConfig != null) {
        connection = HbaseConnectionFactory.create(keygenConfig.getHbaseZk());
        keygenService = new HBaseLockingKeyService(keygenConfig.getOccHbaseConfiguration(), connection, datasetId);
      }
    }

    @SneakyThrows
    @Teardown
    public void teardown() {
      if (connection != null) {
        connection.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> BasicRecord.newBuilder().setId(er.getId()).setCreated(DateTime.now().getMillis()).build())
          .via(BasicInterpreter.interpretGbifId(keygenService))
          .via(BasicInterpreter::interpretBasisOfRecord)
          .via(BasicInterpreter::interpretTypifiedName)
          .via(BasicInterpreter::interpretSex)
          .via(BasicInterpreter::interpretEstablishmentMeans)
          .via(BasicInterpreter::interpretLifeStage)
          .via(BasicInterpreter::interpretTypeStatus)
          .via(BasicInterpreter::interpretIndividualCount)
          .via(BasicInterpreter::interpretReferences)
          .consume(context::output);

      counter.inc();
    }
  }

}
