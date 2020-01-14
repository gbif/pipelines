package org.gbif.pipelines.transforms.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.utils.SupplierFactory;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.geocode.GeocodeResponse;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;

/**
 * Beam level transformations for the DWC Location, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link LocationRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
 * as a source and {@link LocationInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#location</a>
 */
@Slf4j
public class LocationTransform extends Transform<ExtendedRecord, LocationRecord> {

  private final KvConfig kvConfig;
  private KeyValueStore<LatLng, GeocodeResponse> kvStore;
  private PCollectionView<MetadataRecord> metadataView;

  public LocationTransform(KeyValueStore<LatLng, GeocodeResponse> kvStore, KvConfig kvConfig) {
    super(LocationRecord.class, LOCATION, LocationTransform.class.getName(), LOCATION_RECORDS_COUNT);
    this.kvStore = kvStore;
    this.kvConfig = kvConfig;
  }

  public static LocationTransform create() {
    return new LocationTransform(null, null);
  }

  public static LocationTransform create(KvConfig kvConfig) {
    return new LocationTransform(null, kvConfig);
  }

  public static LocationTransform create(KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    return new LocationTransform(kvStore, null);
  }

  public static LocationTransform create(String propertiesPath) {
    KvConfig config = KvConfigFactory.create(Paths.get(propertiesPath), KvConfigFactory.GEOCODE_PREFIX);
    return new LocationTransform(null, config);
  }

  public static LocationTransform create(Properties properties) {
    KvConfig config = KvConfigFactory.create(properties, KvConfigFactory.GEOCODE_PREFIX);
    return new LocationTransform(null, config);
  }

  public SingleOutput<ExtendedRecord, LocationRecord> interpret(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  public LocationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public LocationTransform init() {
    kvStore = SupplierFactory.getInstance(() -> SETUP_FN.apply(kvConfig)).getService();
    return this;
  }

  @Setup
  public void setup() {
    if (kvStore == null) {
      kvStore = SETUP_FN.apply(kvConfig);
    }
  }

  private static final Function<KvConfig, KeyValueStore<LatLng, GeocodeResponse>> SETUP_FN = config -> {
    if (config != null) {
      try {
        ClientConfiguration clientConfig = ClientConfiguration.builder()
            .withBaseApiUrl(config.getBasePath()) //GBIF base API url
            .withFileCacheMaxSizeMb(config.getCacheSizeMb()) //Max file cache size
            .withTimeOut(config.getTimeout()) //Geocode service connection time-out
            .build();

        if (config.getZookeeperUrl() != null && !config.isRestOnly()) {

          CachedHBaseKVStoreConfiguration geocodeKvStoreConfig = CachedHBaseKVStoreConfiguration.builder()
              .withValueColumnQualifier("j") //stores JSON data
              .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                  .withTableName(config.getTableName()) //Geocode KV HBase table
                  .withColumnFamily("v") //Column in which qualifiers are stored
                  .withNumOfKeyBuckets(config.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
                  .withHBaseZk(config.getZookeeperUrl()) //HBase Zookeeper ensemble
                  .build())
              .withCacheCapacity(15_000L)
              .build();

          return GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
        } else {
          return GeocodeKVStoreFactory.simpleGeocodeKVStore(clientConfig);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return null;
  };

  @Teardown
  public void tearDown() {
    if (Objects.nonNull(kvStore)) {
      try {
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KVStore", ex);
      }
    }
  }

  @Override
  public Optional<LocationRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element(), c.sideInput(metadataView)).ifPresent(c::output);
  }

  public Optional<LocationRecord> processElement(ExtendedRecord source, MetadataRecord mdr) {

    LocationRecord lr = LocationRecord.newBuilder()
        .setId(source.getId())
        .setCreated(Instant.now().toEpochMilli())
        .build();

    Optional<LocationRecord> result = Interpretation.from(source)
        .to(lr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(LocationInterpreter.interpretCountryAndCoordinates(kvStore, mdr))
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
        .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }

}
