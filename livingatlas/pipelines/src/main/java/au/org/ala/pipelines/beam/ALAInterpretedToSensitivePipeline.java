package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.SDSCheckKVStoreFactory;
import au.org.ala.kvs.cache.SDSReportKVStoreFactory;
import au.org.ala.kvs.client.SDSConservationServiceFactory;
import au.org.ala.pipelines.transforms.ALASensitiveDataRecordTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/** ALA Beam pipeline to process sensitive data. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAInterpretedToSensitivePipeline {

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sensitive");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    options.setMetaFileName(ValidationUtils.SENSITIVE_METRICS);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
                options.getProperties())
            .get();

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "INTERPRETED_TO_SENSITIVE");

    if (!ValidationUtils.isInterpretationAvailable(options)) {
      log.warn(
          "The dataset can not processed with SDS. Interpretation has not been ran for dataset: {}",
          options.getDatasetId());
      return;
    }

    log.info("1. Delete previous SDS processing.");
    deletePreviousSDS(options);

    log.info("Adding step 1: Options");
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
    UnaryOperator<String> inputPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    UnaryOperator<String> eventPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Event, t, ALL_AVRO);

    UnaryOperator<String> outputPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, id);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();

    // ALA specific
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    ALASensitiveDataRecordTransform alaSensitiveDataRecordTransform =
        ALASensitiveDataRecordTransform.builder()
            .config(config)
            .datasetId(options.getDatasetId())
            .speciesStoreSupplier(SDSCheckKVStoreFactory.getInstanceSupplier(config))
            .reportStoreSupplier(SDSReportKVStoreFactory.getInstanceSupplier(config))
            .conservationServiceSupplier(SDSConservationServiceFactory.getInstanceSupplier(config))
            .erTag(verbatimTransform.getTag())
            .trTag(temporalTransform.getTag())
            .lrTag(locationTransform.getTag())
            .txrTag(null)
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

    PCollection<KV<String, ALATaxonRecord>> inputAlaTaxonCollection =
        p.apply("Read Taxon", alaTaxonomyTransform.read(inputPathFn))
            .apply("Map Taxon to KV", alaTaxonomyTransform.toKv());

    if (ArchiveUtils.isEventCore(options)) {

      inputLocationCollection =
          applyLocationInheritance(eventPathFn, inputPathFn, p, locationTransform);

      inputTemporalCollection =
          applyTemporalInheritance(eventPathFn, inputPathFn, p, temporalTransform);
    }

    KeyedPCollectionTuple<String> inputTuples =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), inputVerbatimCollection)
            .and(temporalTransform.getTag(), inputTemporalCollection)
            .and(locationTransform.getTag(), inputLocationCollection)
            // ALA Specific
            .and(alaTaxonomyTransform.getTag(), inputAlaTaxonCollection);

    log.info("Creating sensitivity records");
    inputTuples
        .apply("Grouping objects", CoGroupByKey.create())
        .apply("Converting to sensitivity records", alaSensitiveDataRecordTransform.interpret())
        .apply("Write to AVRO", alaSensitiveDataRecordTransform.write(outputPathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  private static PCollection<KV<String, LocationRecord>> applyLocationInheritance(
      UnaryOperator<String> eventPathFn,
      UnaryOperator<String> occPathFn,
      Pipeline p,
      LocationTransform transform) {
    // occurrence output keyed by eventID
    PCollection<KV<String, LocationRecord>> eventLocationCollection =
        p.apply("Read Events", transform.read(eventPathFn))
            .apply("Map Taxon to KV", transform.toKv());

    // event output keyed by eventID
    PCollection<KV<String, LocationRecord>> occLocationCollection =
        p.apply("Read Events", transform.read(occPathFn))
            .apply("Map Taxon to KV", transform.toCoreIdKv());

    // joined
    PCollection<KV<String, KV<LocationRecord, LocationRecord>>> joined =
        Join.leftOuterJoin(
            occLocationCollection,
            eventLocationCollection,
            LocationRecord.newBuilder().setId("").build());

    return joined.apply(ParDo.of(new LocationMergeFcn()));
  }

  private static PCollection<KV<String, TemporalRecord>> applyTemporalInheritance(
      UnaryOperator<String> eventPathFn,
      UnaryOperator<String> occPathFn,
      Pipeline p,
      TemporalTransform transform) {
    // event output keyed by eventID
    PCollection<KV<String, TemporalRecord>> occCollection =
        p.apply("Read Events", transform.read(occPathFn))
            .apply("Map Taxon to KV", transform.toCoreIdKv());

    // occurrence output keyed by eventID
    PCollection<KV<String, TemporalRecord>> eventCollection =
        p.apply("Read Events", transform.read(eventPathFn))
            .apply("Map Taxon to KV", transform.toKv());

    // joined
    PCollection<KV<String, KV<TemporalRecord, TemporalRecord>>> joined =
        Join.leftOuterJoin(
            occCollection, eventCollection, TemporalRecord.newBuilder().setId("").build());

    return joined.apply(ParDo.of(new TemporalMergeFcn()));
  }

  static class LocationMergeFcn
      extends DoFn<KV<String, KV<LocationRecord, LocationRecord>>, KV<String, LocationRecord>> {
    @ProcessElement
    public void processElement(
        @Element KV<String, KV<LocationRecord, LocationRecord>> occAndEvent,
        OutputReceiver<KV<String, LocationRecord>> out) {

      LocationRecord occLocationRecord = occAndEvent.getValue().getKey();
      LocationRecord eventLocationRecord = occAndEvent.getValue().getValue();

      if (occLocationRecord.getDecimalLongitude() == null
          && occLocationRecord.getDecimalLatitude() == null) {
        occLocationRecord.setDecimalLongitude(eventLocationRecord.getDecimalLongitude());
        occLocationRecord.setDecimalLatitude(eventLocationRecord.getDecimalLatitude());
        occLocationRecord.setStateProvince(eventLocationRecord.getStateProvince());
        occLocationRecord.setCountryCode(eventLocationRecord.getCountryCode());
      }

      out.output(KV.of(occLocationRecord.getId(), occLocationRecord));
    }
  }

  static class TemporalMergeFcn
      extends DoFn<KV<String, KV<TemporalRecord, TemporalRecord>>, KV<String, TemporalRecord>> {
    @ProcessElement
    public void processElement(
        @Element KV<String, KV<TemporalRecord, TemporalRecord>> occAndEvent,
        OutputReceiver<KV<String, TemporalRecord>> out) {

      TemporalRecord occTemporalRecord = occAndEvent.getValue().getKey();
      TemporalRecord eventTemporalRecord = occAndEvent.getValue().getValue();

      boolean hasMonthInfo = occTemporalRecord.getMonth() != null;
      boolean hasYearInfo = eventTemporalRecord.getYear() != null;

      // extract location & temporal information from
      if (!hasYearInfo && eventTemporalRecord.getYear() != null) {
        occTemporalRecord.setYear(eventTemporalRecord.getYear());
      }

      if (!hasMonthInfo && eventTemporalRecord.getMonth() != null) {
        occTemporalRecord.setMonth(eventTemporalRecord.getMonth());
      }

      out.output(KV.of(occTemporalRecord.getId(), occTemporalRecord));
    }
  }

  public static void deletePreviousSDS(InterpretationPipelineOptions options) {

    String path =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            DwcTerm.Occurrence.simpleName().toLowerCase(),
            "ala_sensitive_data");

    // delete output directories
    FsUtils.deleteIfExist(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);
  }
}
