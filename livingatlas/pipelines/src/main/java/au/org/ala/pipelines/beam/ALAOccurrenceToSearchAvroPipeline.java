package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALAOccurrenceToSearchTransform;
import au.org.ala.pipelines.transforms.ALASensitiveDataRecordTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALAUUIDTransform;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.OccurrenceSearchRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/**
 * Pipeline that generates an AVRO index that will support a Spark SQL query interface for
 * downloads.
 *
 * <p>This pipelines creates: 1) An Event an AVRO export for querying 2) An Interpreted Occurrence
 * export joining 3) A verbatim Occurrence export
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAOccurrenceToSearchAvroPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "index");
    IndexingPipelineOptions options =
        PipelinesOptionsFactory.create(IndexingPipelineOptions.class, combinedArgs);
    run(options);
  }

  public static void run(IndexingPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      IndexingPipelineOptions options, Function<IndexingPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);

    options.setAppName("Event indexing of " + options.getDatasetId());
    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    BasicTransform basicTransform = BasicTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    ALASensitiveDataRecordTransform sensitiveTransform =
        ALASensitiveDataRecordTransform.builder().create();
    // Taxonomy transform for loading occurrence taxonomy info
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();
    ALAUUIDTransform alaUuidTransform = ALAUUIDTransform.create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Metadata", verbatimTransform.read(occurrencesPathFn))
            .apply("Map Event core to KV", verbatimTransform.toKv());

    PCollection<KV<String, ALAUUIDRecord>> uuidCollection =
        p.apply("Read Metadata", alaUuidTransform.read(identifiersPathFn))
            .apply("Map Event core to KV", alaUuidTransform.toKv());

    PCollection<KV<String, ALAAttributionRecord>> attributionCollection =
        p.apply("Read Metadata", alaAttributionTransform.read(occurrencesPathFn))
            .apply("Map Event core to KV", alaAttributionTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Event core", basicTransform.read(occurrencesPathFn))
            .apply("Map Event core to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(occurrencesPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(occurrencesPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    // load the taxonomy from the occurrence records
    PCollection<KV<String, ALATaxonRecord>> taxonCollection =
        p.apply("Read taxa", alaTaxonomyTransform.read(occurrencesPathFn))
            .apply("Map taxa to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, ALASensitivityRecord>> sensitiveCollection =
        p.apply("Read taxa", sensitiveTransform.read(occurrencesPathFn))
            .apply("Map taxa to KV", sensitiveTransform.toKv());

    log.info("Adding step 3: Converting into a json object");
    ParDo.SingleOutput<KV<String, CoGbkResult>, OccurrenceSearchRecord> occSearchAvroFn =
        ALAOccurrenceToSearchTransform.builder()
            .basicRecordTag(basicTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .sensitivityRecordTag(sensitiveTransform.getTag())
            .taxonRecordTag(alaTaxonomyTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .alaAttributionTag(alaAttributionTransform.getTag())
            .alaUuidRecordTTag(alaUuidTransform.getTag())
            .verbatimRecordTag(verbatimTransform.getTag())
            .build()
            .converter();

    PCollection<OccurrenceSearchRecord> occurrenceSearchCollection =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(alaTaxonomyTransform.getTag(), taxonCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(sensitiveTransform.getTag(), sensitiveCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(alaAttributionTransform.getTag(), attributionCollection)
            .and(alaUuidTransform.getTag(), uuidCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", occSearchAvroFn);

    String avroPath =
        String.join(
            "/",
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "search",
            DwcTerm.Occurrence.simpleName().toLowerCase(),
            "search");

    occurrenceSearchCollection.apply(
        AvroIO.write(OccurrenceSearchRecord.class)
            .to(avroPath)
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    if (ArchiveUtils.isEventCore(options)) {
      ALAEventToSearchAvroPipeline eventPipeline = new ALAEventToSearchAvroPipeline();
      eventPipeline.run(options);
    }

    log.info("Pipeline has been finished");
  }
}
