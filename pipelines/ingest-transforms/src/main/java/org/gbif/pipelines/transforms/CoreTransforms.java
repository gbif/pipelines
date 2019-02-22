package org.gbif.pipelines.transforms;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.core.interpreters.core.MetadataInterpreter;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.retrofit.NameMatchServiceSyncClient;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;

/**
 * Contains ParDo functions for Beam, each method returns GBIF transformation (basic, temporal,
 * multimedia, location, metadata, taxonomy). Transformation uses {@link
 * org.gbif.pipelines.core.interpreters} to interpret and convert source data to target data
 *
 * <p>You can apply this functions to your Beam pipeline:
 *
 * <pre>{@code
 * PCollection<ExtendedRecord> records = ...
 * PCollection<TemporalRecord> t = records.apply(ParDo.of(new BasicFn()));
 *
 * }</pre>
 */
public class CoreTransforms {

  private CoreTransforms() {}

  /**
   * ParDo runs sequence of interpretations for {@link TemporalRecord} using {@link ExtendedRecord}
   * as a source and {@link TemporalInterpreter} as interpretation steps
   */
  public static class TemporalFn extends DoFn<ExtendedRecord, TemporalRecord> {

    private final Counter counter = Metrics.counter(CoreTransforms.class, TEMPORAL_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> TemporalRecord.newBuilder().setId(er.getId()).build())
          .via(TemporalInterpreter::interpretEventDate)
          .via(TemporalInterpreter::interpretDateIdentified)
          .via(TemporalInterpreter::interpretModifiedDate)
          .via(TemporalInterpreter::interpretDayOfYear)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link BasicRecord} using {@link ExtendedRecord} as
   * a source and {@link BasicInterpreter} as interpretation steps
   */
  public static class BasicFn extends DoFn<ExtendedRecord, BasicRecord> {

    private final Counter counter = Metrics.counter(CoreTransforms.class, BASIC_RECORDS_COUNT);

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(er -> BasicRecord.newBuilder().setId(er.getId()).build())
          .via(BasicInterpreter::interpretBasisOfRecord)
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

  /**
   * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
   * as a source and {@link LocationInterpreter} as interpretation steps
   */
  public static class LocationFn extends DoFn<ExtendedRecord, LocationRecord> {

    private final Counter counter = Metrics.counter(CoreTransforms.class, LOCATION_RECORDS_COUNT);

    private final KvConfig kvConfig;
    private KeyValueStore<LatLng, String> kvStore;

    public LocationFn(KvConfig kvConfig) {
      this.kvConfig = kvConfig;
    }

    public LocationFn(KeyValueStore<LatLng, String> kvStore) {
      this.kvStore = kvStore;
      this.kvConfig = null;
    }

    public LocationFn(String properties) {
      this.kvConfig = KvConfigFactory.create(KvConfigFactory.GEOCODE_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() throws IOException {
      if (kvConfig != null) {
        HBaseKVStoreConfiguration hBaseKvStoreConfig = HBaseKVStoreConfiguration.builder()
            .withTableName("geocode_kv") //Geocode KV HBase table
            .withColumnFamily("v") //Column in which qualifiers are stored
            .withNumOfKeyBuckets(
                kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations == to # of region servers
            .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
            .build();

        GeocodeKVStoreConfiguration geocodeKvStoreConfig = GeocodeKVStoreConfiguration.builder()
            .withJsonColumnQualifier("j") //stores JSON data
            .withCountryCodeColumnQualifier("c") //stores ISO country code
            .withHBaseKVStoreConfiguration(hBaseKvStoreConfig)
            .build();

        ClientConfiguration clientConfig = ClientConfiguration.builder()
            .withBaseApiUrl(kvConfig.getBasePath()) //GBIF base API url
            .withFileCacheMaxSizeMb(kvConfig.getCacheSizeMb()) //Max file cache size
            .withTimeOut(kvConfig.getTimeout()) //Geocode service connection time-out
            .build();

        kvStore = GeocodeKVStoreFactory.simpleGeocodeKVStore(geocodeKvStoreConfig, clientConfig);
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
          .via(LocationInterpreter::interpretMinimumDepthInMeters)
          .via(LocationInterpreter::interpretMaximumDepthInMeters)
          .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
          .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
          .via(LocationInterpreter::interpretCoordinatePrecision)
          .via(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
          .consume(context::output);

      counter.inc();
    }
  }

  /**
   * ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
   * as a source and {@link MetadataInterpreter} as interpretation steps
   *
   * <p>wsConfig to create a WsConfig object, please use {@link WsConfigFactory}
   */
  public static class MetadataFn extends DoFn<String, MetadataRecord> {

    private final Counter counter = Metrics.counter(CoreTransforms.class, METADATA_RECORDS_COUNT);

    private final WsConfig wsConfig;
    private MetadataServiceClient client;

    public MetadataFn(WsConfig wsConfig) {
      this.wsConfig = wsConfig;
    }

    public MetadataFn(String properties) {
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

  /**
   * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
   * a source and {@link TaxonomyInterpreter} as interpretation steps
   */
  public static class TaxonomyFn extends DoFn<ExtendedRecord, TaxonRecord> {

    private final Counter counter = Metrics.counter(CoreTransforms.class, TAXON_RECORDS_COUNT);

    private final KvConfig kvConfig;
    private KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;

    public TaxonomyFn(KvConfig kvConfig) {
      this.kvConfig = kvConfig;
    }

    public TaxonomyFn(KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
      this.kvStore = kvStore;
      this.kvConfig = null;
    }

    public TaxonomyFn(String properties) {
      this.kvConfig = KvConfigFactory.create(KvConfigFactory.TAXONOMY_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() throws IOException {
      if (kvConfig != null) {
        HBaseKVStoreConfiguration storeConfig = HBaseKVStoreConfiguration.builder()
            .withTableName("name_usage_kv") //Geocode KV HBase table
            .withColumnFamily("v") //Column in which qualifiers are stored
            .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations
            .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
            .build();

        NameMatchServiceSyncClient nameMatchClient = new NameMatchServiceSyncClient(ClientConfiguration.builder()
            .withBaseApiUrl(kvConfig.getBasePath()) //GBIF base API url
            .withFileCacheMaxSizeMb(kvConfig.getCacheSizeMb()) //Max file cache size
            .withTimeOut(kvConfig.getTimeout()) //Geocode service connection time-out
            .build());

        NameUsageMatchKVConfiguration matchConfig = NameUsageMatchKVConfiguration.builder()
            .withJsonColumnQualifier("j") //stores JSON data
            .withHBaseKVStoreConfiguration(storeConfig).build();

        kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, nameMatchClient);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Interpretation.from(context::element)
          .to(TaxonRecord.newBuilder()::build)
          .via(TaxonomyInterpreter.taxonomyInterpreter(kvStore))
          // the id is null when there is an error in the interpretation. In these
          // cases we do not write the taxonRecord because it is totally empty.
          .consume(v -> Optional.ofNullable(v.getId()).ifPresent(id -> context.output(v)));

      counter.inc();
    }
  }
}
