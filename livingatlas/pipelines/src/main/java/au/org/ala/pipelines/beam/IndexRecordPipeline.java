package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.transforms.*;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationResult;
import au.org.ala.utils.ValidationUtils;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.slf4j.MDC;

/**
 * Pipeline for creating an index of the records in AVRO.
 *
 * <p>This pipeline works 2 ways depending on configuration.
 *
 * <ul>
 *   <li>Produces AVRO records ready to be index into SOLR in a separate pipeline see {@link
 *       IndexRecordToSolrPipeline}</lu>
 *   <li>Writes directly into SOLR
 * </ul>
 *
 * This pipeline uses the HTTP SOLR api to index records. This is currently limited to using HTTP
 * 1.1 due Apache Beam not yet using the HTTP2 api.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IndexRecordPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    IndexingPipelineOptions options =
        PipelinesOptionsFactory.create(IndexingPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    System.exit(0);
  }

  public static void run(IndexingPipelineOptions options) throws Exception {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    ValidationResult valid = ValidationUtils.checkReadyForIndexing(options);
    if (!valid.getValid()) {
      log.error(
          "The dataset can not be indexed. See logs for more details: {}", valid.getMessage());
      return;
    }

    FileSystem fs =
        FileSystemFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()))
            .getFs(options.getInputPath());

    final long lastLoadedDate =
        ValidationUtils.metricsModificationTime(
            fs,
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt(),
            ValidationUtils.VERBATIM_METRICS);
    final long lastProcessedDate =
        ValidationUtils.metricsModificationTime(
            fs,
            options.getInputPath(),
            options.getDatasetId(),
            options.getAttempt(),
            ValidationUtils.INTERPRETATION_METRICS);

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, ALL_AVRO);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    ALABasicTransform basicTransform = ALABasicTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();

    // ALA specific
    ALAUUIDTransform alaUuidTransform = ALAUUIDTransform.create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();
    ALASensitiveDataRecordTransform alaSensitiveDataRecordTransform =
        ALASensitiveDataRecordTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", basicTransform.read(pathFn))
            .apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection = null;
    if (options.getIncludeGbifTaxonomy()) {
      taxonCollection =
          p.apply("Read Taxon", taxonomyTransform.read(pathFn))
              .apply("Map Taxon to KV", taxonomyTransform.toKv());
    }

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    // ALA Specific
    PCollection<KV<String, ALAUUIDRecord>> alaUUidCollection =
        p.apply("Read Taxon", alaUuidTransform.read(identifiersPathFn))
            .apply("Map Taxon to KV", alaUuidTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> alaTaxonCollection =
        p.apply("Read Taxon", alaTaxonomyTransform.read(pathFn))
            .apply("Map Taxon to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, ALAAttributionRecord>> alaAttributionCollection =
        p.apply("Read attribution", alaAttributionTransform.read(pathFn))
            .apply("Map attribution to KV", alaAttributionTransform.toKv());

    // load images
    PCollection<KV<String, ImageRecord>> alaImageRecords = null;
    if (options.getIncludeImages()) {
      alaImageRecords = getLoadImageServiceRecords(options, p);
    }

    // load taxon profiles
    PCollection<KV<String, TaxonProfile>> alaTaxonProfileRecords = null;
    if (options.getIncludeSpeciesLists()) {
      alaTaxonProfileRecords = SpeciesListPipeline.generateTaxonProfileCollection(p, options);
    }

    PCollection<KV<String, ALASensitivityRecord>> alaSensitiveDataCollection = null;
    if (options.getIncludeSensitiveDataChecks()) {
      alaSensitiveDataCollection =
          p.apply("Read sensitive data", alaSensitiveDataRecordTransform.read(pathFn))
              .apply("Map sensitive data to KV", alaSensitiveDataRecordTransform.toKv());
    }

    final TupleTag<ImageRecord> imageRecordTupleTag = new TupleTag<ImageRecord>() {};
    final TupleTag<TaxonProfile> speciesListsRecordTupleTag = new TupleTag<TaxonProfile>() {};

    IndexRecordTransform indexRecordTransform =
        IndexRecordTransform.create(
            verbatimTransform.getTag(),
            basicTransform.getTag(),
            temporalTransform.getTag(),
            locationTransform.getTag(),
            options.getIncludeGbifTaxonomy() ? taxonomyTransform.getTag() : null,
            alaTaxonomyTransform.getTag(),
            multimediaTransform.getTag(),
            alaAttributionTransform.getTag(),
            alaUuidTransform.getTag(),
            options.getIncludeImages() ? imageRecordTupleTag : null,
            options.getIncludeSpeciesLists() ? speciesListsRecordTupleTag : null,
            options.getIncludeSensitiveDataChecks()
                ? alaSensitiveDataRecordTransform.getTag()
                : null,
            options.getDatasetId(),
            lastLoadedDate,
            lastProcessedDate);

    log.info("Adding step 3: Converting into a json object");
    ParDo.SingleOutput<KV<String, CoGbkResult>, IndexRecord> alaSolrDoFn =
        indexRecordTransform.converter();

    KeyedPCollectionTuple<String> kpct =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // ALA Specific
            .and(alaUuidTransform.getTag(), alaUUidCollection)
            .and(alaTaxonomyTransform.getTag(), alaTaxonCollection)
            .and(alaAttributionTransform.getTag(), alaAttributionCollection);

    if (options.getIncludeSpeciesLists()) {
      kpct = kpct.and(speciesListsRecordTupleTag, alaTaxonProfileRecords);
    }

    if (options.getIncludeImages()) {
      kpct = kpct.and(imageRecordTupleTag, alaImageRecords);
    }

    if (options.getIncludeGbifTaxonomy()) {
      kpct = kpct.and(taxonomyTransform.getTag(), taxonCollection);
    }

    if (options.getIncludeSensitiveDataChecks()) {
      kpct = kpct.and(alaSensitiveDataRecordTransform.getTag(), alaSensitiveDataCollection);
    }

    PCollection<IndexRecord> indexRecordCollection =
        kpct.apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to Solr doc", alaSolrDoFn);

    String outputPath =
        options.getAllDatasetsInputPath()
            + "/index-record/"
            + options.getDatasetId()
            + "/"
            + options.getDatasetId();

    // clean previous runs
    ALAFsUtils.deleteIfExist(fs, outputPath);

    // write to AVRO file instead....
    indexRecordCollection.apply(
        AvroIO.write(IndexRecord.class).to(outputPath).withSuffix(".avro").withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    // run occurrence AVRO pipeline
    ALAOccurrenceToSearchAvroPipeline.run(options);

    log.info("Pipeline has been finished");
  }

  /** Load image service records for a dataset. */
  private static PCollection<KV<String, ImageRecord>> getLoadImageServiceRecords(
      IndexingPipelineOptions options, Pipeline p) {
    PCollection<KV<String, ImageRecord>> alaImageServiceRecords;
    alaImageServiceRecords =
        p.apply(
                AvroIO.read(ImageRecord.class)
                    .from(
                        String.join(
                            "/",
                            options.getTargetPath(),
                            options.getDatasetId().trim(),
                            options.getAttempt().toString(),
                            "images",
                            "*.avro")))
            .apply(
                MapElements.into(new TypeDescriptor<KV<String, ImageRecord>>() {})
                    .via((ImageRecord tr) -> KV.of(tr.getId(), tr)));
    return alaImageServiceRecords;
  }
}
