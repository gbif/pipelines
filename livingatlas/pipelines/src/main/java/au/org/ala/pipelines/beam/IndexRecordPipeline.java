package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.transforms.ALAAttributionTransform;
import au.org.ala.pipelines.transforms.ALABasicTransform;
import au.org.ala.pipelines.transforms.ALASensitiveDataRecordTransform;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALAUUIDTransform;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
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
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageServiceRecord;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaxonProfile;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.slf4j.MDC;

/**
 * ALA Beam pipeline for creating an index of the records. This pipeline works 2 ways depending on
 * configuration.
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

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "speciesLists", "index");
    ALASolrPipelineOptions options =
        PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    System.exit(0);
  }

  public static void run(ALASolrPipelineOptions options) throws Exception {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    ValidationResult valid = ValidationUtils.checkReadyForIndexing(options);
    if (!valid.getValid()) {
      log.error(
          "The dataset can not be indexed. See logs for more details: {}", valid.getMessage());
      return;
    }

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    UnaryOperator<String> identifiersPathFn =
        t -> ALAFsUtils.buildPathIdentifiersUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    ALABasicTransform basicTransform = ALABasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();

    // ALA specific
    ALAUUIDTransform alaUuidTransform = ALAUUIDTransform.create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    ALAAttributionTransform alaAttributionTransform = ALAAttributionTransform.builder().create();
    ALASensitiveDataRecordTransform alaSensitiveDataRecordTransform =
        ALASensitiveDataRecordTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

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

    // ALA Specific
    PCollection<KV<String, ALAUUIDRecord>> alaUUidCollection =
        p.apply("Read UUID", alaUuidTransform.read(identifiersPathFn))
            .apply("Map UUID to KV", alaUuidTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> alaTaxonCollection =
        p.apply("Read ALA Taxon", alaTaxonomyTransform.read(pathFn))
            .apply("Map ALA Taxon to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, ALAAttributionRecord>> alaAttributionCollection =
        p.apply("Read attribution", alaAttributionTransform.read(pathFn))
            .apply("Map attribution to KV", alaAttributionTransform.toKv());

    // load images
    PCollection<KV<String, ImageServiceRecord>> alaImageServiceRecords = null;
    if (options.getIncludeImages()) {
      alaImageServiceRecords = getLoadImageServiceRecords(options, p);
    }

    // load taxon profiles
    PCollection<KV<String, TaxonProfile>> alaTaxonProfileRecords = null;
    if (options.getIncludeSpeciesLists()) {
      alaTaxonProfileRecords = SpeciesListPipeline.generateTaxonProfileCollection(p, options);
    }

    PCollection<KV<String, ALASensitivityRecord>> alaSensitiveDataCollection = null;
    if (options.getIncludeSensitiveData()) {
      alaSensitiveDataCollection =
          p.apply("Read sensitive data", alaSensitiveDataRecordTransform.read(pathFn))
              .apply("Map sensitive to KV", alaSensitiveDataRecordTransform.toKv());
    }

    final TupleTag<ImageServiceRecord> imageServiceRecordTupleTag =
        new TupleTag<ImageServiceRecord>() {};

    final TupleTag<TaxonProfile> speciesListsRecordTupleTag = new TupleTag<TaxonProfile>() {};

    IndexRecordTransform.IndexRecordTransformBuilder recordTransformBuilder =
        IndexRecordTransform.builder()
            .erTag(verbatimTransform.getTag())
            .brTag(basicTransform.getTag())
            .trTag(temporalTransform.getTag())
            .lrTag(locationTransform.getTag())
            .atxrTag(alaTaxonomyTransform.getTag())
            .aarTag(alaAttributionTransform.getTag())
            .urTag(alaUuidTransform.getTag())
            .metadataView(metadataView)
            .datasetID(options.getDatasetId());

    KeyedPCollectionTuple<String> kpct =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // ALA Specific
            .and(alaUuidTransform.getTag(), alaUUidCollection)
            .and(alaTaxonomyTransform.getTag(), alaTaxonCollection)
            .and(alaAttributionTransform.getTag(), alaAttributionCollection);

    if (options.getIncludeImages()) {
      recordTransformBuilder.isTag(imageServiceRecordTupleTag);
      kpct = kpct.and(imageServiceRecordTupleTag, alaImageServiceRecords);
    }
    if (options.getIncludeSpeciesLists()) {
      recordTransformBuilder.tpTag(speciesListsRecordTupleTag);
      kpct = kpct.and(speciesListsRecordTupleTag, alaTaxonProfileRecords);
    }
    if (options.getIncludeSensitiveData()) {
      recordTransformBuilder.srTag(alaSensitiveDataRecordTransform.getTag());
      kpct = kpct.and(alaSensitiveDataRecordTransform.getTag(), alaSensitiveDataCollection);
    }
    if (options.getIncludeGbifTaxonomy()) {
      recordTransformBuilder.txrTag(taxonomyTransform.getTag());
      kpct = kpct.and(taxonomyTransform.getTag(), taxonCollection);
    }

    PCollection<IndexRecord> indexRecordCollection =
        kpct.apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to Solr doc", recordTransformBuilder.build().converter());

    String outputPath =
        options.getAllDatasetsInputPath()
            + "/index-record/"
            + options.getDatasetId()
            + "/"
            + options.getDatasetId();

    // clean previous runs
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());
    ALAFsUtils.deleteIfExist(fs, outputPath);

    // write to AVRO file instead....
    indexRecordCollection.apply(
        AvroIO.write(IndexRecord.class).to(outputPath).withSuffix(".avro").withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  /** Load image service records for a dataset. */
  private static PCollection<KV<String, ImageServiceRecord>> getLoadImageServiceRecords(
      ALASolrPipelineOptions options, Pipeline p) {
    PCollection<KV<String, ImageServiceRecord>> alaImageServiceRecords;
    alaImageServiceRecords =
        p.apply(
                AvroIO.read(ImageServiceRecord.class)
                    .from(
                        String.join(
                            "/",
                            options.getTargetPath(),
                            options.getDatasetId().trim(),
                            options.getAttempt().toString(),
                            "images",
                            "*.avro")))
            .apply(
                MapElements.into(new TypeDescriptor<KV<String, ImageServiceRecord>>() {})
                    .via((ImageServiceRecord tr) -> KV.of(tr.getId(), tr)));
    return alaImageServiceRecords;
  }
}
