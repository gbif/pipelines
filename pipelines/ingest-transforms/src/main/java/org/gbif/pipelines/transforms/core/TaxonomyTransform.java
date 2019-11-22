package org.gbif.pipelines.transforms.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedHBaseKVStoreConfiguration;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;

/**
 * Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class TaxonomyTransform extends Transform<ExtendedRecord, TaxonRecord> {

  private final KvConfig kvConfig;
  private KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;

  private TaxonomyTransform(KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore, KvConfig kvConfig) {
    super(TaxonRecord.class, TAXONOMY, TaxonomyTransform.class.getName(), TAXON_RECORDS_COUNT);
    this.kvStore = kvStore;
    this.kvConfig = kvConfig;
  }

  public static TaxonomyTransform create() {
    return new TaxonomyTransform(null, null);
  }

  public static TaxonomyTransform create(KvConfig kvConfig) {
    return new TaxonomyTransform(null, kvConfig);
  }

  public static TaxonomyTransform create(KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
    return new TaxonomyTransform(kvStore, null);
  }

  public static TaxonomyTransform create(String propertiesPath) {
    return new TaxonomyTransform(null, KvConfigFactory.create(Paths.get(propertiesPath), KvConfigFactory.TAXONOMY_PREFIX));
  }

  public static TaxonomyTransform create(Properties propertiesPath) {
    return new TaxonomyTransform(null, KvConfigFactory.create(propertiesPath, KvConfigFactory.TAXONOMY_PREFIX));
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<TaxonRecord, KV<String, TaxonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
        .via((TaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  @Setup
  public void setup() throws IOException {
    if (kvConfig != null) {

      ClientConfiguration clientConfiguration = ClientConfiguration.builder()
          .withBaseApiUrl(kvConfig.getBasePath()) //GBIF base API url
          .withFileCacheMaxSizeMb(kvConfig.getCacheSizeMb()) //Max file cache size
          .withTimeOut(kvConfig.getTimeout()) //Geocode service connection time-out
          .build();

      if (kvConfig.getZookeeperUrl() != null && !kvConfig.isRestOnly()) {

        CachedHBaseKVStoreConfiguration matchConfig = CachedHBaseKVStoreConfiguration.builder()
            .withValueColumnQualifier("j") //stores JSON data
            .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                .withTableName(kvConfig.getTableName()) //Geocode KV HBase table
                .withColumnFamily("v") //Column in which qualifiers are stored
                .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations
                .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
                .build())
            .withCacheCapacity(15_000L)
            .build();

        kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, clientConfiguration);
      } else {
        kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
      }

    }
  }

  @Teardown
  public void tearDown() {
    if (Objects.nonNull(kvStore)) {
      try {
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KV Store", ex);
      }
    }
  }

  @Override
  public Optional<TaxonRecord> convert(ExtendedRecord source) {
    TaxonRecord tr = TaxonRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build();

    Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(TaxonomyInterpreter.taxonomyInterpreter(kvStore));

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return tr.getId() == null ? Optional.empty() : Optional.of(tr);
  }

}
