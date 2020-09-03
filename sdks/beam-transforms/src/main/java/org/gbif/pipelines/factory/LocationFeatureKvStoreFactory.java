package org.gbif.pipelines.factory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.ReadOnlyHBaseStore;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocationFeatureKvStoreFactory {

  public static SerializableSupplier<KeyValueStore<LatLng, String>> createSupplier(
      KvConfig kvConfig) {
    return () -> create(kvConfig);
  }

  @SneakyThrows
  public static KeyValueStore<LatLng, String> create(KvConfig kvConfig) {
    CachedHBaseKVStoreConfiguration hBaseKVStoreConfiguration =
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
                    .build())
            .withCacheCapacity(15_000L)
            .build();

    return ReadOnlyHBaseStore.<LatLng, String>builder()
        .withHBaseStoreConfiguration(hBaseKVStoreConfiguration.getHBaseKVStoreConfiguration())
        .withResultMapper(
            result -> Bytes.toString(result.getValue(Bytes.toBytes("v"), Bytes.toBytes("json"))))
        .build();
  }
}
