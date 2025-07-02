package org.gbif.pipelines.factory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration.Builder;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.LoaderRetryConfig;
import org.gbif.kvs.hbase.ReadOnlyHBaseStore;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationFeatureKvStoreFactory {

  public static SerializableSupplier<KeyValueStore<GeocodeRequest, String>> createSupplier(
      KvConfig kvConfig) {
    return () -> create(kvConfig);
  }

  @SneakyThrows
  public static KeyValueStore<GeocodeRequest, String> create(KvConfig kvConfig) {
    Builder configBuilder =
        CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("json") // stores JSON data
            .withHBaseKVStoreConfiguration(
                HBaseKVStoreConfiguration.builder()
                    .withTableName(kvConfig.getTableName()) // Geocode KV HBase table
                    .withColumnFamily("v") // Column in which qualifiers are stored
                    .withNumOfKeyBuckets(
                        kvConfig
                            .getNumOfKeyBuckets()) // Buckets for salted key generations == to # of
                    // region servers
                    .withHBaseZk(kvConfig.getZkConnectionString()) // HBase Zookeeper ensemble
                    .withHBaseZnode(kvConfig.getHbaseZnode())
                    .build())
            .withCacheCapacity(25_000L);

    KvConfig.LoaderRetryConfig retryConfig = kvConfig.getLoaderRetryConfig();
    if (retryConfig != null) {
      configBuilder.withLoaderRetryConfig(
          new LoaderRetryConfig(
              retryConfig.getMaxAttempts(),
              retryConfig.getInitialIntervalMillis(),
              retryConfig.getMultiplier(),
              retryConfig.getRandomizationFactor()));
    }

    return ReadOnlyHBaseStore.<GeocodeRequest, String>builder()
        .withHBaseStoreConfiguration(configBuilder.build().getHBaseKVStoreConfiguration())
        .withResultMapper(
            result -> Bytes.toString(result.getValue(Bytes.toBytes("v"), Bytes.toBytes("json"))))
        .build();
  }
}
