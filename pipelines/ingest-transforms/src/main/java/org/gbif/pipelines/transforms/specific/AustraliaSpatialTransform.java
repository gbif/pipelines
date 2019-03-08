package org.gbif.pipelines.transforms.specific;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.AustraliaSpatialInterpreter;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUSTRALIA_SPATIAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUSTRALIA_SPATIAL;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the Australia location, reads an avro, writes an avro, maps from value to keyValue
 * and transforms form {@link LocationRecord} to {@link AustraliaSpatialRecord}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AustraliaSpatialTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = AUSTRALIA_SPATIAL.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#AUSTRALIA_SPATIAL}, else returns empty {@link
   * PCollection<LocationRecord>}
   */
  public static CheckTransforms<LocationRecord> check(List<String> types) {
    return CheckTransforms.create(LocationRecord.class, checkRecordType(types, AUSTRALIA_SPATIAL));
  }

  /** Maps {@link AustraliaSpatialRecord} to key value, where key is {@link AustraliaSpatialRecord#getId} */
  public static MapElements<AustraliaSpatialRecord, KV<String, AustraliaSpatialRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AustraliaSpatialRecord>>() {})
        .via((AustraliaSpatialRecord asr) -> KV.of(asr.getId(), asr));
  }

  /**
   * Reads avro files from path, which contains {@link AustraliaSpatialRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<AustraliaSpatialRecord> read(String path) {
    return AvroIO.read(AustraliaSpatialRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link AustraliaSpatialRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AustraliaSpatialTransform#BASE_NAME
   */
  public static AvroIO.Read<AustraliaSpatialRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link AustraliaSpatialRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<AustraliaSpatialRecord> write(String toPath) {
    return AvroIO.write(AustraliaSpatialRecord.class)
        .to(toPath)
        .withSuffix(Pipeline.AVRO_EXTENSION)
        .withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link AustraliaSpatialRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link AustraliaSpatialTransform#BASE_NAME}
   */
  public static AvroIO.Write<AustraliaSpatialRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link AustraliaSpatialRecord}
   */
  public static SingleOutput<LocationRecord, AustraliaSpatialRecord> interpret(KvConfig kvConfig) {
    return ParDo.of(new Interpreter(kvConfig));
  }

  /**
   * Creates an {@link Interpreter} for {@link AustraliaSpatialRecord}
   */
  public static SingleOutput<LocationRecord, AustraliaSpatialRecord> interpret(KeyValueStore<LatLng, String> kvStore) {
    return ParDo.of(new Interpreter(kvStore));
  }

  /**
   * Creates an {@link Interpreter} for {@link AustraliaSpatialRecord}
   */
  public static SingleOutput<LocationRecord, AustraliaSpatialRecord> interpret(String properties) {
    return ParDo.of(new Interpreter(properties));
  }

  /**
   * ParDo runs sequence of interpretations for {@link AustraliaSpatialRecord} using {@link LocationRecord} as
   * a source and {@link AustraliaSpatialInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<LocationRecord, AustraliaSpatialRecord> {

    private final Counter counter = Metrics.counter(AustraliaSpatialTransform.class, AUSTRALIA_SPATIAL_RECORDS_COUNT);

    private final KvConfig kvConfig;
    private KeyValueStore<LatLng, String> kvStore;

    public Interpreter(KvConfig kvConfig) {
      this.kvConfig = kvConfig;
    }

    public Interpreter(KeyValueStore<LatLng, String> kvStore) {
      this.kvStore = kvStore;
      this.kvConfig = null;
    }

    public Interpreter(String properties) {
      this.kvConfig = KvConfigFactory.create(KvConfigFactory.AUSTRALIA_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() throws IOException {
      if (kvConfig != null) {

        GeocodeKVStoreConfiguration geocodeKvStoreConfig = GeocodeKVStoreConfiguration.builder()
            .withJsonColumnQualifier("json") //stores JSON data
            .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                .withTableName(kvConfig.getTableName()) //Geocode KV HBase table
                .withColumnFamily("v") //Column in which qualifiers are stored
                .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
                .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
                .build())
            .withCacheCapacity(15_000L)
            .build();

        kvStore = GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig);

      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(lr -> AustraliaSpatialRecord.newBuilder().setId(lr.getId()).build())
          .when(lr -> Optional.ofNullable(lr.getCountryCode())
              .filter(c -> c.equals(Country.AUSTRALIA.getIso2LetterCode()))
              .filter(c -> new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()).isValid())
              .isPresent())
          .via(AustraliaSpatialInterpreter.interpret(kvStore))
          .consume(context::output);

      counter.inc();
    }
  }

}
