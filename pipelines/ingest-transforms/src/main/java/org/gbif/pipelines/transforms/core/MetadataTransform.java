package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.util.List;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MetadataInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.transforms.CheckTransforms;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PCollection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the GBIF metadata, read an avro, write an avro, from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link MetadataRecord}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetadataTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  /**
   * Checks if list contains {@link RecordType#METADATA}, else returns empty {@link PCollection<String>}
   */
  public static CheckTransforms<String> check(List<String> types) {
    return CheckTransforms.create(String.class, checkRecordType(types, METADATA));
  }

  /**
   * Reads avro files from path, which contains {@link MetadataRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<MetadataRecord> read(String path) {
    return AvroIO.read(MetadataRecord.class).from(path);
  }

  /**
   * Writes {@link MetadataRecord} *.avro files to path, without splitting the file, uses Snappy
   * codec compression by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<MetadataRecord> write(String toPath) {
    return AvroIO.write(MetadataRecord.class)
        .to(toPath)
        .withSuffix(Pipeline.AVRO_EXTENSION)
        .withCodec(BASE_CODEC)
        .withoutSharding();
  }

  /**
   * Creates an {@link Interpreter} for {@link MetadataRecord}
   */
  public static SingleOutput<String, MetadataRecord> interpret(WsConfig wsConfig) {
    return ParDo.of(new Interpreter(wsConfig));
  }

  /**
   * Creates an {@link Interpreter} for {@link MetadataRecord}
   */
  public static SingleOutput<String, MetadataRecord> interpret(String properties) {
    return ParDo.of(new Interpreter(properties));
  }

  /**
   * ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
   * as a source and {@link MetadataInterpreter} as interpretation steps
   *
   * <p>wsConfig to create a WsConfig object, please use {@link WsConfigFactory}
   */
  public static class Interpreter extends DoFn<String, MetadataRecord> {

    private final Counter counter = Metrics.counter(MetadataTransform.class, METADATA_RECORDS_COUNT);

    private final WsConfig wsConfig;
    private MetadataServiceClient client;

    public Interpreter(WsConfig wsConfig) {
      this.wsConfig = wsConfig;
    }

    public Interpreter(String properties) {
      this.wsConfig = WsConfigFactory.create(WsConfigFactory.METADATA_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() {
      client = MetadataServiceClient.create(wsConfig);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(id -> MetadataRecord.newBuilder().setId(id).build())
          .via(MetadataInterpreter.interpret(client))
          .consume(context::output);

      counter.inc();
    }
  }
}
