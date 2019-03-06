package org.gbif.pipelines.transforms.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.transforms.CheckTransforms;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;
import org.gbif.rest.client.species.retrofit.NameMatchServiceSyncClient;

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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;
import static org.gbif.pipelines.transforms.CheckTransforms.checkRecordType;

/**
 * Beam level transformations for the DWC Taxon, read an avro, write an avro, from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonomyTransform {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  /**
   * Checks if list contains {@link RecordType#TAXONOMY}, else returns empty {@link PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> check(List<String> types) {
    return CheckTransforms.create(ExtendedRecord.class, checkRecordType(types, TAXONOMY));
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public static MapElements<TaxonRecord, KV<String, TaxonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
        .via((TaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  /**
   * Reads avro files from path, which contains {@link TaxonRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<TaxonRecord> read(String path) {
    return AvroIO.read(TaxonRecord.class).from(path);
  }

  /**
   * Writes {@link TaxonRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<TaxonRecord> write(String toPath) {
    return AvroIO.write(TaxonRecord.class).to(toPath).withSuffix(Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Creates an {@link Interpreter} for {@link TaxonRecord}
   */
  public static SingleOutput<ExtendedRecord, TaxonRecord> interpret(KvConfig kvConfig) {
    return ParDo.of(new Interpreter(kvConfig));
  }

  /**
   * Creates an {@link Interpreter} for {@link TaxonRecord}
   */
  public static SingleOutput<ExtendedRecord, TaxonRecord> interpret(KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
    return ParDo.of(new Interpreter(kvStore));
  }

  /**
   * Creates an {@link Interpreter} for {@link TaxonRecord}
   */
  public static SingleOutput<ExtendedRecord, TaxonRecord> interpret(String properties) {
    return ParDo.of(new Interpreter(properties));
  }

  /**
   * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
   * a source and {@link TaxonomyInterpreter} as interpretation steps
   */
  public static class Interpreter extends DoFn<ExtendedRecord, TaxonRecord> {

    private final Counter counter = Metrics.counter(TaxonomyTransform.class, TAXON_RECORDS_COUNT);

    private final KvConfig kvConfig;
    private KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;

    public Interpreter(KvConfig kvConfig) {
      this.kvConfig = kvConfig;
    }

    public Interpreter(KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
      this.kvStore = kvStore;
      this.kvConfig = null;
    }

    public Interpreter(String properties) {
      this.kvConfig = KvConfigFactory.create(KvConfigFactory.TAXONOMY_PREFIX, Paths.get(properties));
    }

    @Setup
    public void setup() throws IOException {
      if (kvConfig != null) {

        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
            .withBaseApiUrl(kvConfig.getBasePath()) //GBIF base API url
            .withFileCacheMaxSizeMb(kvConfig.getCacheSizeMb()) //Max file cache size
            .withTimeOut(kvConfig.getTimeout()) //Geocode service connection time-out
            .build();

        if (kvConfig.getZookeeperUrl() != null) {

          NameMatchServiceSyncClient nameMatchClient = new NameMatchServiceSyncClient(clientConfiguration);

          NameUsageMatchKVConfiguration matchConfig = NameUsageMatchKVConfiguration.builder()
              .withJsonColumnQualifier("j") //stores JSON data
              .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                  .withTableName(kvConfig.getTableName()) //Geocode KV HBase table
                  .withColumnFamily("v") //Column in which qualifiers are stored
                  .withNumOfKeyBuckets(kvConfig.getNumOfKeyBuckets()) //Buckets for salted key generations
                  .withHBaseZk(kvConfig.getZookeeperUrl()) //HBase Zookeeper ensemble
                  .build())
              .build();

          kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(matchConfig, nameMatchClient);
        } else {
          kvStore = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(clientConfiguration);
        }

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
