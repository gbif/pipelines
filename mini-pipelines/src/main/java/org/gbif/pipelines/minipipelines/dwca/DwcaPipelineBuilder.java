package org.gbif.pipelines.minipipelines.dwca;

import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.gbif.pipelines.assembling.GbifInterpretationType;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.Kv2Value;
import org.gbif.pipelines.transform.RecordTransform;
import org.gbif.pipelines.transform.record.*;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;
import org.gbif.pipelines.utils.FsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.function.BiConsumer;

import static org.gbif.pipelines.minipipelines.dwca.DwcaMiniPipelineOptions.GbifEnv.*;
import static org.gbif.pipelines.minipipelines.dwca.DwcaMiniPipelineOptions.PipelineStep.DWCA_TO_AVRO;
import static org.gbif.pipelines.minipipelines.dwca.DwcaMiniPipelineOptions.PipelineStep.INTERPRET;

/**
 * Builder to create a Pipeline that works with Dwc-A files. It adds different steps to the pipeline
 * dependending on the {@link DwcaMiniPipelineOptions#getPipelineStep()}.
 *
 * <p>This class is intended to be used internally, so it should always be package-private.
 */
class DwcaPipelineBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaPipelineBuilder.class);

  private static final String TEMP_DEFAULT = "tmp";

  private DwcaPipelineBuilder() {}

  static Pipeline buildPipeline(DwcaMiniPipelineOptions options) {
    LOG.info("Starting pipeline building");

    // create pipeline
    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class);

    // STEP 1: Read the DwC-A using our custom reader
    LOG.info("Adding step 1: reading dwca");
    PCollection<ExtendedRecord> rawRecords =
        pipeline.apply(
            "Read from Darwin Core Archive",
            Paths.get(options.getInputPath()).toFile().isDirectory()
                ? DwCAIO.Read.withPaths(options.getInputPath())
                : DwCAIO.Read.withPaths(options.getInputPath(), OutputWriter.getTempDir(options)));

    // STEP 2: Remove duplicates
    LOG.info("Adding step 2: removing duplicates");
    UniqueOccurrenceIdTransform uniquenessTransform =
        UniqueOccurrenceIdTransform.create().withAvroCoders(pipeline);
    PCollectionTuple uniqueTuple = rawRecords.apply(uniquenessTransform);
    PCollection<ExtendedRecord> verbatimRecords = uniqueTuple.get(uniquenessTransform.getDataTag());

    // TODO: count number of records read to log it??

    // only write if it'' the final the step or the intermediate outputs are not ignored
    if (DWCA_TO_AVRO == options.getPipelineStep() || !options.getIgnoreIntermediateOutputs()) {
      OutputWriter.writeToAvro(
          verbatimRecords,
          ExtendedRecord.class,
          options,
          FsUtils.buildPathString(OutputWriter.getRootPath(options), "verbatim"));
    }

    if (DWCA_TO_AVRO == options.getPipelineStep()) {
      LOG.info("Returning pipeline for step {}", DWCA_TO_AVRO);
      return pipeline;
    }

    // STEP 3: Interpretations
    LOG.info("Adding step 3: interpretations");
    final Config wsConfig = WsConfigFactory.getConfig(options.getGbifEnv());

    // Taxonomy
    LOG.info("Adding taxonomy interpretation");
    TaxonRecordTransform taxonTransform = TaxonRecordTransform.create(wsConfig);
    taxonTransform.withAvroCoders(pipeline);
    PCollectionTuple taxonRecordTuple =
        verbatimRecords.apply("Taxonomy interpretation", taxonTransform);
    PCollection<KV<String, TaxonRecord>> interpretedTaxonRecords =
        taxonRecordTuple.get(taxonTransform.getDataTag());
    OutputWriter.writeInterpretationResult(
        taxonRecordTuple,
        TaxonRecord.class,
        taxonTransform,
        options,
        GbifInterpretationType.TAXONOMY);

    // Location
    LOG.info("Adding location interpretation");
    LocationRecordTransform locationTransform = LocationRecordTransform.create(wsConfig);
    locationTransform.withAvroCoders(pipeline);
    PCollectionTuple locationRecordTuple =
        verbatimRecords.apply("Location interpretation", locationTransform);
    PCollection<KV<String, LocationRecord>> interpretedLocationRecords =
        locationRecordTuple.get(locationTransform.getDataTag());
    OutputWriter.writeInterpretationResult(
        locationRecordTuple,
        LocationRecord.class,
        locationTransform,
        options,
        GbifInterpretationType.LOCATION);

    // Temporal
    LOG.info("Adding temporal interpretation");
    TemporalRecordTransform temporalTransform = TemporalRecordTransform.create();
    temporalTransform.withAvroCoders(pipeline);
    PCollectionTuple temporalRecordTuple =
        verbatimRecords.apply("Temporal interpretation", temporalTransform);
    PCollection<KV<String, TemporalRecord>> interpretedTemporalRecords =
        temporalRecordTuple.get(temporalTransform.getDataTag());
    OutputWriter.writeInterpretationResult(
        temporalRecordTuple,
        TemporalRecord.class,
        temporalTransform,
        options,
        GbifInterpretationType.TEMPORAL);

    // Common
    LOG.info("Adding common interpretation");
    InterpretedExtendedRecordTransform interpretedRecordTransform =
        InterpretedExtendedRecordTransform.create();
    interpretedRecordTransform.withAvroCoders(pipeline);
    PCollectionTuple interpretedRecordTuple =
        verbatimRecords.apply("Common interpretation", interpretedRecordTransform);
    PCollection<KV<String, InterpretedExtendedRecord>> interpretedRecords =
        interpretedRecordTuple.get(interpretedRecordTransform.getDataTag());
    OutputWriter.writeInterpretationResult(
        interpretedRecordTuple,
        InterpretedExtendedRecord.class,
        interpretedRecordTransform,
        options,
        GbifInterpretationType.COMMON);

    // Multimedia
    LOG.info("Adding multimedia interpretation");
    MultimediaRecordTransform multimediaTransform = MultimediaRecordTransform.create();
    multimediaTransform.withAvroCoders(pipeline);
    PCollectionTuple multimediaRecordTuple =
        verbatimRecords.apply("Multimedia interpretation", multimediaTransform);
    PCollection<KV<String, MultimediaRecord>> interpretedMultimediaRecords =
        multimediaRecordTuple.get(multimediaTransform.getDataTag());
    OutputWriter.writeInterpretationResult(
        multimediaRecordTuple,
        MultimediaRecord.class,
        multimediaTransform,
        options,
        GbifInterpretationType.MULTIMEDIA);

    if (INTERPRET == options.getPipelineStep()) {
      LOG.info("Returning pipeline for step {}", INTERPRET);
      return pipeline;
    }

    // STEP 4: index in ES
    LOG.info("Adding step 4: indexing in ES");

    // create tags
    final TupleTag<ExtendedRecord> extendedRecordTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<InterpretedExtendedRecord> interRecordTag =
        new TupleTag<InterpretedExtendedRecord>() {};
    final TupleTag<TemporalRecord> temporalTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> locationTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> taxonomyTag = new TupleTag<TaxonRecord>() {};
    final TupleTag<MultimediaRecord> multimediaTag = new TupleTag<MultimediaRecord>() {};

    // convert extended records to KV collection
    PCollection<KV<String, ExtendedRecord>> verbatimRecordsMapped =
        verbatimRecords.apply(
            "Map verbatim records to KV",
            MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
                .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    // group all the collections
    PCollection<KV<String, CoGbkResult>> groupedCollection =
        KeyedPCollectionTuple.of(interRecordTag, interpretedRecords)
            .and(temporalTag, interpretedTemporalRecords)
            .and(locationTag, interpretedLocationRecords)
            .and(taxonomyTag, interpretedTaxonRecords)
            .and(multimediaTag, interpretedMultimediaRecords)
            .and(extendedRecordTag, verbatimRecordsMapped)
            .apply(CoGroupByKey.create());

    LOG.info("Adding step 3: Converting to a flat object");
    PCollection<String> resultCollection =
        groupedCollection.apply(
            "Merge objects",
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CoGbkResult value = c.element().getValue();
                    String key = c.element().getKey();
                    InterpretedExtendedRecord interRecord =
                        value.getOnly(
                            interRecordTag,
                            InterpretedExtendedRecord.newBuilder().setId(key).build());
                    TemporalRecord temporal =
                        value.getOnly(temporalTag, TemporalRecord.newBuilder().setId(key).build());
                    LocationRecord location =
                        value.getOnly(locationTag, LocationRecord.newBuilder().setId(key).build());
                    TaxonRecord taxon =
                        value.getOnly(taxonomyTag, TaxonRecord.newBuilder().setId(key).build());
                    MultimediaRecord multimedia =
                        value.getOnly(
                            multimediaTag, MultimediaRecord.newBuilder().setId(key).build());
                    ExtendedRecord extendedRecord =
                        value.getOnly(
                            extendedRecordTag, ExtendedRecord.newBuilder().setId(key).build());
                    c.output(
                        EsSchemaConverter.toIndex(
                            interRecord, temporal, location, taxon, multimedia, extendedRecord));
                  }
                }));

    ElasticsearchIO.ConnectionConfiguration esBeamConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getESHosts(), options.getESIndexName(), "record");

    resultCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esBeamConfig)
            .withMaxBatchSizeBytes(options.getESMaxBatchSize())
            .withMaxBatchSize(options.getESMaxBatchSizeBytes()));

    return pipeline;
  }

  static class OutputWriter {

    private OutputWriter() {}

    private static <T> void writeToAvro(
        PCollection<T> records, Class<T> avroClass, DwcaMiniPipelineOptions options, String path) {
      records.apply(
          AvroIO.write(avroClass)
              .to(FileSystems.matchNewResource(path, false))
              .withSuffix(".avro")
              .withCodec(CodecFactory.snappyCodec())
              .withTempDirectory(FileSystems.matchNewResource(getTempDir(options), true)));
    }

    private static <T> void writeInterpretationResult(
        PCollectionTuple collectionTuple,
        Class<T> avroClass,
        RecordTransform<ExtendedRecord, T> transform,
        DwcaMiniPipelineOptions options,
        GbifInterpretationType interpretationType) {

      // only write if it'' the final the step or the intermediate outputs are not ignored
      if (INTERPRET == options.getPipelineStep() || !options.getIgnoreIntermediateOutputs()) {

        String rootInterpretationPath =
            FsUtils.buildPathString(getRootPath(options), interpretationType.name().toLowerCase());

        // write interpeted data
        String interpretedPath = FsUtils.buildPathString(rootInterpretationPath, "interpreted");
        writeToAvro(
            collectionTuple.get(transform.getDataTag()).apply(Kv2Value.create()),
            avroClass,
            options,
            interpretedPath);

        // write issues
        String issuesPath = FsUtils.buildPathString(rootInterpretationPath, "issues");
        writeToAvro(
            collectionTuple.get(transform.getIssueTag()).apply(Kv2Value.create()),
            OccurrenceIssue.class,
            options,
            issuesPath);
      }
    }

    private static String getRootPath(DwcaMiniPipelineOptions options) {
      return FsUtils.buildPathString(
          options.getTargetPath(), options.getDatasetId(), String.valueOf(options.getAttempt()));
    }

    static String getTempDir(DwcaMiniPipelineOptions options) {
      return Strings.isNullOrEmpty(options.getTempLocation())
          ? FsUtils.buildPathString(options.getTargetPath(), TEMP_DEFAULT)
          : options.getTempLocation();
    }
  }

  private static class EsSchemaConverter {
    // FIXME: this is temporary till we define the final ES schema

    private EsSchemaConverter() {}

    /** Assemble main object json with nested structure */
    private static String toIndex(
        InterpretedExtendedRecord interRecord,
        TemporalRecord temporal,
        LocationRecord location,
        TaxonRecord taxon,
        MultimediaRecord multimedia,
        ExtendedRecord extendedRecord) {

      StringBuilder builder = new StringBuilder();
      BiConsumer<String, String> f =
          (k, v) -> builder.append("\"").append(k).append("\":").append(v);

      builder.append("{\"id\":\"").append(extendedRecord.getId()).append("\"").append(",");

      f.accept("raw", extendedRecord.toString());
      builder.append(",");
      f.accept("common", interRecord.toString());
      builder.append(",");
      f.accept("temporal", temporal.toString());
      builder.append(",");
      f.accept("location", location.toString());
      builder.append(",");
      f.accept("taxon", taxon.toString());
      builder.append(",");
      f.accept("multimedia", multimedia.toString());
      builder.append("}");

      return builder
          .toString()
          .replaceAll("http://rs.tdwg.org/dwc/terms/", "")
          .replaceAll("http://rs.gbif.org/terms/1.0/", "");
    }
  }

  private static class WsConfigFactory {

    private static final String DEV_URL = "https://api.gbif-dev.org/";
    private static final String UAT_URL = "https://api.gbif-uat.org/";
    private static final String PROD_URL = "https://api.gbif.org/";

    private WsConfigFactory() {}

    static Config getConfig(DwcaMiniPipelineOptions.GbifEnv env) {
      if (env == DEV) {
        return HttpConfigFactory.createConfigFromUrl(DEV_URL);
      }
      if (env == UAT) {
        return HttpConfigFactory.createConfigFromUrl(UAT_URL);
      }
      if (env == PROD) {
        return HttpConfigFactory.createConfigFromUrl(PROD_URL);
      }

      throw new IllegalArgumentException(env + " environment not supported");
    }
  }
}
