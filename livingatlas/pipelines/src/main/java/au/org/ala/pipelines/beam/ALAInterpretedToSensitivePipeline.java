package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.SDSCheckKVStoreFactory;
import au.org.ala.kvs.client.SDSConservationServiceFactory;
import au.org.ala.pipelines.transforms.ALASensitiveDataTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALAUUIDTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/** ALA Beam pipeline to process sensitive data. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAInterpretedToSensitivePipeline {
  public static final boolean USE_GBIF_TAXONOMY = false;

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sensitive");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) throws IOException {
    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "INTERPRETED_TO_GENERALISED");

    log.info("Adding step 1: Options");
    UnaryOperator<String> inputPathFn =
        t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    //    * "{targetPath}/{datasetId}/{attempt}/generalised/{name}/interpret-{uniqueId}"
    UnaryOperator<String> outputPathFn =
        t -> ALAFsUtils.buildPathGeneralisedUsingTargetPath(options, t, id);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();

    // ALA specific
    ALAUUIDTransform alaUuidTransform = ALAUUIDTransform.create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    ALASensitiveDataTransform alaSensitiveDataTransform =
        ALASensitiveDataTransform.builder()
            .datasetId(options.getDatasetId())
            .speciesStoreSupplier(SDSCheckKVStoreFactory.getInstanceSupplier(config))
            .dataResourceStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .conservationServiceSupplier(SDSConservationServiceFactory.getInstanceSupplier(config))
            .erTag(verbatimTransform.getTag())
            .trTag(temporalTransform.getTag())
            .lrTag(locationTransform.getTag())
            .txrTag(USE_GBIF_TAXONOMY ? taxonomyTransform.getTag() : null)
            .atxrTag(alaTaxonomyTransform.getTag())
            .create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<KV<String, ExtendedRecord>> inputVerbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(inputPathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, TemporalRecord>> inputTemporalCollection =
        p.apply("Read Temporal", temporalTransform.read(inputPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> inputLocationCollection =
        p.apply("Read Location", locationTransform.read(inputPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> inputTaxonCollection = null;
    if (USE_GBIF_TAXONOMY) {
      inputTaxonCollection =
          p.apply("Read Location", taxonomyTransform.read(inputPathFn))
              .apply("Map Location to KV", taxonomyTransform.toKv());
    }

    // ALA Specific
    PCollection<KV<String, ALAUUIDRecord>> inputAlaUUidCollection =
        p.apply("Read Taxon", alaUuidTransform.read(identifiersPathFn))
            .apply("Map Taxon to KV", alaUuidTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> inputAlaTaxonCollection =
        p.apply("Read Taxon", alaTaxonomyTransform.read(inputPathFn))
            .apply("Map Taxon to KV", alaTaxonomyTransform.toKv());

    KeyedPCollectionTuple<String> inputTuples =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), inputVerbatimCollection)
            .and(temporalTransform.getTag(), inputTemporalCollection)
            .and(locationTransform.getTag(), inputLocationCollection)
            // ALA Specific
            .and(alaUuidTransform.getTag(), inputAlaUUidCollection)
            .and(alaTaxonomyTransform.getTag(), inputAlaTaxonCollection);
    if (USE_GBIF_TAXONOMY)
      inputTuples = inputTuples.and(taxonomyTransform.getTag(), inputTaxonCollection);

    log.info("Creating sensitivity records");
    PCollection<ALASensitivityRecord> sensitivityRecords =
        inputTuples
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Converting to sensitvity records", alaSensitiveDataTransform.interpret());

    log.info("Writing sensitivity records");
    sensitivityRecords.apply(alaSensitiveDataTransform.write(outputPathFn));

    log.info("Generalising other records");
    PCollection<KV<String, ALASensitivityRecord>> sensitivityCollection =
        sensitivityRecords.apply(alaSensitiveDataTransform.toKv());

    KeyedPCollectionTuple.of(verbatimTransform.getTag(), inputVerbatimCollection)
        .and(alaSensitiveDataTransform.getTag(), sensitivityCollection)
        .apply(CoGroupByKey.create())
        .apply(alaSensitiveDataTransform.rewriter(ExtendedRecord.class, verbatimTransform.getTag()))
        .apply(verbatimTransform.write(outputPathFn).withoutSharding());

    KeyedPCollectionTuple.of(temporalTransform.getTag(), inputTemporalCollection)
        .and(alaSensitiveDataTransform.getTag(), sensitivityCollection)
        .apply(CoGroupByKey.create())
        .apply(alaSensitiveDataTransform.rewriter(TemporalRecord.class, temporalTransform.getTag()))
        .apply(temporalTransform.write(outputPathFn));

    KeyedPCollectionTuple.of(locationTransform.getTag(), inputLocationCollection)
        .and(alaSensitiveDataTransform.getTag(), sensitivityCollection)
        .apply(CoGroupByKey.create())
        .apply(alaSensitiveDataTransform.rewriter(LocationRecord.class, locationTransform.getTag()))
        .apply(locationTransform.write(outputPathFn));

    if (USE_GBIF_TAXONOMY) {
      KeyedPCollectionTuple.of(taxonomyTransform.getTag(), inputTaxonCollection)
          .and(alaSensitiveDataTransform.getTag(), sensitivityCollection)
          .apply(CoGroupByKey.create())
          .apply(alaSensitiveDataTransform.rewriter(TaxonRecord.class, taxonomyTransform.getTag()))
          .apply(taxonomyTransform.write(outputPathFn));
    }

    KeyedPCollectionTuple.of(alaTaxonomyTransform.getTag(), inputAlaTaxonCollection)
        .and(alaSensitiveDataTransform.getTag(), sensitivityCollection)
        .apply(CoGroupByKey.create())
        .apply(
            alaSensitiveDataTransform.rewriter(ALATaxonRecord.class, alaTaxonomyTransform.getTag()))
        .apply(alaTaxonomyTransform.write(outputPathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
