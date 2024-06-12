package uk.org.nbn.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.pipelines.util.VersionInfo;
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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;
import uk.org.nbn.pipelines.transforms.NBNAccessControlRecordTransform;

/** ALA Beam pipeline to process sensitive data. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NBNInterpretedToAccessControlledPipeline {

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sensitive");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    options.setMetaFileName("access-control-metrics.yml");
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
    MDC.put("step", "INTERPRETED_TO_ACCESS_CONTROL");

    if (!ValidationUtils.isInterpretationAvailable(options)) {
      log.warn(
          "The dataset can not processed with SDS. Interpretation has not been ran for dataset: {}",
          options.getDatasetId());
      return;
    }

    log.info("1. Delete previous Access Control processing.");
    deletePreviousAccessControl(options);

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
    LocationTransform locationTransform = LocationTransform.builder().create();
    // TODO HMJ osgrid eventCoreTransform = EventCoreTransform.builder().create();

    NBNAccessControlRecordTransform nbnAccessControlRecordTransform =
        NBNAccessControlRecordTransform.builder()
            .config(config)
            .datasetId(options.getDatasetId())
            .erTag(verbatimTransform.getTag())
            .lrTag(locationTransform.getTag())
            .create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<KV<String, ExtendedRecord>> inputVerbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(inputPathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, LocationRecord>> inputLocationCollection =
        p.apply("Read Location", locationTransform.read(inputPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    KeyedPCollectionTuple<String> inputTuples =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), inputVerbatimCollection)
            .and(locationTransform.getTag(), inputLocationCollection);

    log.info("Creating access controlled records");
    inputTuples
        .apply("Grouping objects", CoGroupByKey.create())
        .apply(
            "Converting to access controlled records", nbnAccessControlRecordTransform.interpret())
        .apply("Write to AVRO", nbnAccessControlRecordTransform.write(outputPathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  public static void deletePreviousAccessControl(InterpretationPipelineOptions options) {

    String path =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            DwcTerm.Occurrence.simpleName().toLowerCase(),
            "nbn_access_controlled_data");

    // delete output directories
    FsUtils.deleteIfExist(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);
  }
}
