package org.gbif.pipelines.transforms.specific;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

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
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

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

  private final Counter counter = Metrics.counter(AustraliaSpatialTransform.class, AUSTRALIA_SPATIAL_RECORDS_COUNT);

  private final KvConfig kvConfig;
  private KeyValueStore<LatLng, String> kvStore;

  private AustraliaSpatialTransform(KeyValueStore<LatLng, String> kvStore, KvConfig kvConfig) {
    super(AustraliaSpatialRecord.class, AUSTRALIA_SPATIAL);
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

  public static AustraliaSpatialTransform create(String properties) {
    KvConfig config = KvConfigFactory.create(KvConfigFactory.AUSTRALIA_PREFIX, Paths.get(properties));
    return new AustraliaSpatialTransform(null, config);
  }

  /** Maps {@link AustraliaSpatialRecord} to key value, where key is {@link AustraliaSpatialRecord#getId} */
  public MapElements<AustraliaSpatialRecord, KV<String, AustraliaSpatialRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AustraliaSpatialRecord>>() {})
        .via((AustraliaSpatialRecord ar) -> KV.of(ar.getId(), ar));
  }

  @Setup
  public void setup() throws IOException {
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

  @ProcessElement
  public void processElement(ProcessContext context) {
    Interpretation.from(context::element)
        .to(lr -> AustraliaSpatialRecord.newBuilder()
            .setId(lr.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build())
        .when(lr -> Optional.ofNullable(lr.getCountryCode())
            .filter(c -> c.equals(Country.AUSTRALIA.getIso2LetterCode()))
            .filter(c -> new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()).isValid())
            .isPresent())
        .via(AustraliaSpatialInterpreter.interpret(kvStore))
        .consume(context::output);

    counter.inc();

  }

}
