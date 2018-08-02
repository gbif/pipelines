package org.gbif.pipelines.minipipelines.dwca;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.indexing.MergeRecords2JsonTransform;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationRecordTransform;
import org.gbif.pipelines.transform.record.MetadataRecordTransform;
import org.gbif.pipelines.transform.record.MultimediaRecordTransform;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;
import org.gbif.pipelines.utils.FsUtils;

import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.assembling.GbifInterpretationType.COMMON;
import static org.gbif.pipelines.assembling.GbifInterpretationType.LOCATION;
import static org.gbif.pipelines.assembling.GbifInterpretationType.MULTIMEDIA;
import static org.gbif.pipelines.assembling.GbifInterpretationType.TAXONOMY;
import static org.gbif.pipelines.assembling.GbifInterpretationType.TEMPORAL;
import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.PipelineStep.DWCA_TO_AVRO;
import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.PipelineStep.INTERPRET;
import static org.gbif.pipelines.minipipelines.dwca.OutputWriter.getRootPath;

/**
 * Builder to create a Pipeline that works with Dwc-A files. It adds different steps to the pipeline
 * dependending on the {@link DwcaPipelineOptions#getPipelineStep()}.
 *
 * <p>This class is intended to be used internally, so it should always be package-private.
 */
class DwcaPipelineBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaPipelineBuilder.class);

  private DwcaPipelineBuilder() {}

  static Pipeline buildPipeline(DwcaPipelineOptions options) {
    LOG.info("Starting pipeline building");

    // create pipeline
    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class);

    LOG.info("STEP 1: Interpret metadata");
    final Config metaWsConfig = WsConfigFactory.getConfig(options.getGbifEnv(), "v1/");
    MetadataRecordTransform metadataTransform = MetadataRecordTransform.create(metaWsConfig).withAvroCoders(pipeline);
    PCollection<String> metaCollection = pipeline.apply(Create.of(options.getDatasetId()));
    PCollectionTuple metadataTuple = metaCollection.apply("Metadata interpretation", metadataTransform);
    if (INTERPRET == options.getPipelineStep() || !options.getIgnoreIntermediateOutputs()) {
      String path = FsUtils.buildPathString(getRootPath(options), "metadata");
      metadataTuple
          .get(metadataTransform.getDataTag())
          .apply(Values.create())
          .apply(AvroIO.write(MetadataRecord.class).to(path).withSuffix(".avro").withoutSharding());
    }

    LOG.info("STEP 2: Read the DwC-A using our custom reader");
    PCollection<ExtendedRecord> rawRecords =
        pipeline.apply(
            "Read from Darwin Core Archive",
            Paths.get(options.getInputPath()).toFile().isDirectory()
                ? DwCAIO.Read.withPaths(options.getInputPath())
                : DwCAIO.Read.withPaths(options.getInputPath(), OutputWriter.getTempDir(options)));

    LOG.info("Adding step 3: removing duplicates");
    UniqueOccurrenceIdTransform uniquenessTransform = UniqueOccurrenceIdTransform.create().withAvroCoders(pipeline);
    PCollectionTuple uniqueTuple = rawRecords.apply(uniquenessTransform);
    PCollection<ExtendedRecord> verbatimRecords = uniqueTuple.get(uniquenessTransform.getDataTag());

    // TODO: count number of records read to log it??
    // only write if it'' the final the step or the intermediate outputs are not ignored
    if (DWCA_TO_AVRO == options.getPipelineStep() || !options.getIgnoreIntermediateOutputs()) {
      String path = FsUtils.buildPathString(getRootPath(options), "verbatim");
      OutputWriter.writeToAvro(verbatimRecords, ExtendedRecord.class, options, path);
    }

    if (DWCA_TO_AVRO == options.getPipelineStep()) {
      LOG.info("Returning pipeline for step {}", DWCA_TO_AVRO);
      return pipeline;
    }

    LOG.info("Adding step 4: interpretations");
    final Config wsConfig = WsConfigFactory.getConfig(options.getGbifEnv());

    // Taxonomy
    LOG.info("-Adding taxonomy interpretation");
    TaxonRecordTransform taxonTransform = TaxonRecordTransform.create(wsConfig).withAvroCoders(pipeline);
    PCollectionTuple taxonRecordTuple = verbatimRecords.apply("Taxonomy interpretation", taxonTransform);
    OutputWriter.writeInterpretationResult(taxonRecordTuple, TaxonRecord.class, taxonTransform, options, TAXONOMY);

    // Location
    LOG.info("-Adding location interpretation");
    LocationRecordTransform locationTransform = LocationRecordTransform.create(wsConfig).withAvroCoders(pipeline);
    PCollectionTuple locationTuple = verbatimRecords.apply("Location interpretation", locationTransform);
    OutputWriter.writeInterpretationResult(locationTuple, LocationRecord.class, locationTransform, options, LOCATION);

    // Temporal
    LOG.info("-Adding temporal interpretation");
    TemporalRecordTransform temporalTransform = TemporalRecordTransform.create().withAvroCoders(pipeline);
    PCollectionTuple temporalTuple = verbatimRecords.apply("Temporal interpretation", temporalTransform);
    OutputWriter.writeInterpretationResult(temporalTuple, TemporalRecord.class, temporalTransform, options, TEMPORAL);

    // Common
    LOG.info("-Adding common interpretation");
    InterpretedExtendedRecordTransform interpretedTransform = InterpretedExtendedRecordTransform.create().withAvroCoders(pipeline);
    PCollectionTuple interpretedTuple = verbatimRecords.apply("Common interpretation", interpretedTransform);
    OutputWriter.writeInterpretationResult(interpretedTuple, InterpretedExtendedRecord.class, interpretedTransform, options, COMMON);

    // Multimedia
    LOG.info("-Adding multimedia interpretation");
    MultimediaRecordTransform multimediaTransform = MultimediaRecordTransform.create().withAvroCoders(pipeline);
    PCollectionTuple multimediaTuple = verbatimRecords.apply("Multimedia interpretation", multimediaTransform);
    OutputWriter.writeInterpretationResult(multimediaTuple, MultimediaRecord.class, multimediaTransform, options, MULTIMEDIA);

    if (INTERPRET == options.getPipelineStep()) {
      LOG.info("Returning pipeline for step {}", INTERPRET);
      return pipeline;
    }

    LOG.info("Adding step 4: Converting to a json object");
    MergeRecords2JsonTransform jsonTransform = MergeRecords2JsonTransform.create().withAvroCoders(pipeline);
    PCollectionTuple tuple = PCollectionTuple.of(jsonTransform.getExtendedRecordTag(), verbatimRecords)
      .and(jsonTransform.getInterKvTag(), interpretedTuple.get(interpretedTransform.getDataTag()))
      .and(jsonTransform.getLocationKvTag(), locationTuple.get(locationTransform.getDataTag()))
      .and(jsonTransform.getMultimediaKvTag(), multimediaTuple.get(multimediaTransform.getDataTag()))
      .and(jsonTransform.getTaxonomyKvTag(), taxonRecordTuple.get(taxonTransform.getDataTag()))
      .and(jsonTransform.getTemporalKvTag(), temporalTuple.get(temporalTransform.getDataTag()))
      .and(jsonTransform.getMetadataKvTag(), metadataTuple.get(metadataTransform.getDataTag()));

    PCollection<String> resultCollection = tuple.apply("Merge object to Json", jsonTransform);

    LOG.info("Adding step 5: indexing in ES");
    ElasticsearchIO.ConnectionConfiguration esBeamConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getESHosts(), options.getESIndexName(), "record");

    resultCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esBeamConfig)
            .withMaxBatchSizeBytes(options.getESMaxBatchSizeBytes())
            .withMaxBatchSize(options.getESMaxBatchSize()));

    return pipeline;
  }
}
