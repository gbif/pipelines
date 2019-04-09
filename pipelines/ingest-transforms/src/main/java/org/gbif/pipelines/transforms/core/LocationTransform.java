package org.gbif.pipelines.transforms.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.UnaryOperator;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.transforms.CheckTransforms;
import org.gbif.rest.client.configuration.ClientConfiguration;

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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the DWC Location, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link LocationRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#location</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();
  private static final String BASE_NAME = LOCATION.name().toLowerCase();

  /**
   * Checks if list contains {@link RecordType#LOCATION}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, LOCATION));
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public static MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  /**
   * Reads avro files from path, which contains {@link LocationRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<LocationRecord> read(String path) {
    return AvroIO.read(LocationRecord.class).from(path);
  }

  /**
   * Reads avro files from path, which contains {@link LocationRecord}
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link LocationTransform#BASE_NAME}
   */
  public static AvroIO.Read<LocationRecord> read(UnaryOperator<String> pathFn) {
    return read(pathFn.apply(BASE_NAME));
  }

  /**
   * Writes {@link LocationRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<LocationRecord> write(String toPath) {
    return AvroIO.write(LocationRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link LocationRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link LocationTransform#BASE_NAME}
   */
  public static AvroIO.Write<LocationRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Creates an {@link Interpreter} for {@link LocationRecord}
   */
  public static SingleOutput<ExtendedRecord, LocationRecord> interpret() {
    return ParDo.of(new Interpreter());
  }

  /**
   * Creates an {@link Interpreter} for {@link LocationRecord}
   */
  public static SingleOutput<ExtendedRecord, LocationRecord> interpret(KvConfig kvConfig) {
    return ParDo.of(new Interpreter(kvConfig));
  }

  /**
   * Creates an {@link Interpreter} for {@link LocationRecord}
   */
  public static SingleOutput<ExtendedRecord, LocationRecord> interpret(KeyValueStore<LatLng, String> kvStore) {
    return ParDo.of(new Interpreter(kvStore));
  }

  /**
   * Creates an {@link Interpreter} for {@link LocationRecord}
   */
  public static SingleOutput<ExtendedRecord, LocationRecord> interpret(String properties) {
    return ParDo.of(new Interpreter(properties));
  }

  /**
   * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
   * as a source and {@link LocationInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, LocationRecord> {

    private final Counter counter = Metrics.counter(LocationTransform.class, LOCATION_RECORDS_COUNT);

    private final KvConfig kvConfig;
    private KeyValueStore<LatLng, String> kvStore;

    public Interpreter() {
      this.kvConfig = null;
      this.kvStore = null;
    }

    public Interpreter(KvConfig kvConfig) {
      this.kvConfig = kvConfig;
    }

    public Interpreter(KeyValueStore<LatLng, String> kvStore) {
      this.kvStore = kvStore;
      this.kvConfig = null;
    }

    public Interpreter(String properties) {
      this.kvConfig = KvConfigFactory.create(KvConfigFactory.GEOCODE_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() throws IOException {
      if (kvConfig != null) {

        ClientConfiguration clientConfig = ClientConfiguration.builder()
            .withBaseApiUrl(kvConfig.getBasePath()) //GBIF base API url
            .withFileCacheMaxSizeMb(kvConfig.getCacheSizeMb()) //Max file cache size
            .withTimeOut(kvConfig.getTimeout()) //Geocode service connection time-out
            .build();

        if (kvConfig.getZookeeperUrl() != null) {

          GeocodeKVStoreConfiguration geocodeKvStoreConfig = GeocodeKVStoreConfiguration.builder()
              .withJsonColumnQualifier("j") //stores JSON data
              .withCountryCodeColumnQualifier("c") //stores ISO country code
              .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                  .withTableName(kvConfig.getTableName()) //Geocode KV HBase table
                  .withColumnFamily("v") //Column in which qualifiers are stored
                  .withNumOfKeyBuckets(
                      kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
                  .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
                  .build())
              .withCacheCapacity(15_000L)
              .build();

          kvStore = GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
        } else {
          kvStore = GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
        }

      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> LocationRecord.newBuilder().setId(er.getId()).build())
          .via(LocationInterpreter.interpretCountryAndCoordinates(kvStore))
          .via(LocationInterpreter::interpretContinent)
          .via(LocationInterpreter::interpretWaterBody)
          .via(LocationInterpreter::interpretStateProvince)
          .via(LocationInterpreter::interpretMinimumElevationInMeters)
          .via(LocationInterpreter::interpretMaximumElevationInMeters)
          .via(LocationInterpreter::interpretElevation)
          .via(LocationInterpreter::interpretMinimumDepthInMeters)
          .via(LocationInterpreter::interpretMaximumDepthInMeters)
          .via(LocationInterpreter::interpretDepth)
          .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
          .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
          .via(LocationInterpreter::interpretCoordinatePrecision)
          .via(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
          .consume(context::output);

      counter.inc();
    }
  }

}
