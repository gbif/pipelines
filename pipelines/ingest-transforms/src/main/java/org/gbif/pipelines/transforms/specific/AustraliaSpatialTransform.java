package org.gbif.pipelines.transforms.specific;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.ReadOnlyHBaseStore;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.AustraliaSpatialInterpreter;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.parsers.config.factory.KvConfigFactory;
import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUSTRALIA_SPATIAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUSTRALIA_SPATIAL;

/**
 * Beam level transformations for the Australia location, reads an avro, writes an avro, maps from value to keyValue
 * and transforms form {@link LocationRecord} to {@link AustraliaSpatialRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link AustraliaSpatialRecord} using {@link LocationRecord} as
 * a source and {@link AustraliaSpatialInterpreter} as interpretation steps
 */
@Slf4j
public class AustraliaSpatialTransform extends Transform<LocationRecord, AustraliaSpatialRecord> {

  private final KvConfig kvConfig;
  private KeyValueStore<LatLng, String> kvStore;

  private AustraliaSpatialTransform(KeyValueStore<LatLng, String> kvStore, KvConfig kvConfig) {
    super(AustraliaSpatialRecord.class, AUSTRALIA_SPATIAL, AustraliaSpatialTransform.class.getName(), AUSTRALIA_SPATIAL_RECORDS_COUNT);
    this.kvStore = kvStore;
    this.kvConfig = kvConfig;
  }

  public static AustraliaSpatialTransform create() {
    return new AustraliaSpatialTransform(null, null);
  }

  public static AustraliaSpatialTransform create(KvConfig kvConfig) {
    return new AustraliaSpatialTransform(null, kvConfig);
  }

  public static AustraliaSpatialTransform create(KeyValueStore<LatLng, String> kvStore) {
    return new AustraliaSpatialTransform(kvStore, null);
  }

  public static AustraliaSpatialTransform create(String propertiesPath) {
    KvConfig config = KvConfigFactory.create(Paths.get(propertiesPath), KvConfigFactory.AUSTRALIA_PREFIX);
    return new AustraliaSpatialTransform(null, config);
  }

  public static AustraliaSpatialTransform create(Properties properties) {
    KvConfig config = KvConfigFactory.create(properties, KvConfigFactory.AUSTRALIA_PREFIX);
    return new AustraliaSpatialTransform(null, config);
  }

  /** Maps {@link AustraliaSpatialRecord} to key value, where key is {@link AustraliaSpatialRecord#getId} */
  public MapElements<AustraliaSpatialRecord, KV<String, AustraliaSpatialRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AustraliaSpatialRecord>>() {})
        .via((AustraliaSpatialRecord ar) -> KV.of(ar.getId(), ar));
  }

  public AustraliaSpatialTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public AustraliaSpatialTransform init() {
    setup();
    return this;
  }

  @SneakyThrows
  @Setup
  public void setup() {
    if (kvConfig != null) {

      CachedHBaseKVStoreConfiguration hBaseKVStoreConfiguration = CachedHBaseKVStoreConfiguration.builder()
          .withValueColumnQualifier("json") //stores JSON data
          .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
              .withTableName(kvConfig.getTableName()) //Geocode KV HBase table
              .withColumnFamily("v") //Column in which qualifiers are stored
              .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
              .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
              .build())
          .withCacheCapacity(15_000L)
          .build();

      kvStore = ReadOnlyHBaseStore.<LatLng, String>builder()
          .withHBaseStoreConfiguration(hBaseKVStoreConfiguration.getHBaseKVStoreConfiguration())
          .withResultMapper(result -> Bytes.toString(result.getValue(Bytes.toBytes("v"), Bytes.toBytes("json"))))
          .build();

    }
  }

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
  public Optional<AustraliaSpatialRecord> convert(LocationRecord source) {
    return Interpretation.from(source)
        .to(lr -> AustraliaSpatialRecord.newBuilder()
            .setId(lr.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build())
        .when(lr -> Optional.ofNullable(lr.getCountryCode())
            .filter(c -> c.equals(Country.AUSTRALIA.getIso2LetterCode()))
            .filter(c -> new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()).isValid())
            .isPresent())
        .via(AustraliaSpatialInterpreter.interpret(kvStore))
        .get();
  }

}
