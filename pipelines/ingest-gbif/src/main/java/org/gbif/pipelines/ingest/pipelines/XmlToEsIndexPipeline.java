package org.gbif.pipelines.ingest.pipelines;

import java.util.Properties;

import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.common.beam.XmlIO;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.EsIndexUtils;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.config.factory.LockConfigFactory;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.GbifJsonTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.metadata.TaggedValuesTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads XML files and converts to {@link ExtendedRecord}
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
 *      {@link MetadataRecord},
 *      {@link BasicRecord},
 *      {@link TemporalRecord},
 *      {@link MultimediaRecord},
 *      {@link ImageRecord},
 *      {@link AudubonRecord},
 *      {@link MeasurementOrFactRecord},
 *      {@link TaxonRecord},
 *      {@link LocationRecord}
 *    3) Joins objects
 *    4) Converts to json model (resources/elasticsearch/es-occurrence-schema.json)
 *    5) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=XML_TO_ES_INDEX \
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --inputPath=/some/path/to/input/0057a720-17c9-4658-971e-9578f3577cf5/*.xml
 * --targetPath=/some/path/to/input/
 * --esHosts=http://ADDRESS,http://ADDRESS,http://ADDRESS:9200
 * --esIndexName=pipeline
 * --runner=SparkRunner
 * --properties=/path/ws.properties
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class XmlToEsIndexPipeline {

  public static void main(String[] args) {
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);
    run(options);
  }

  public static void run(DwcaPipelineOptions options) {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();
    boolean tripletValid = options.isTripletValid();
    boolean useExtendedRecordId = options.isUseExtendedRecordId();
    boolean skipRegisrtyCalls = options.isSkipRegisrtyCalls();
    String endPointType = options.getEndPointType();

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());

    EsIndexUtils.createIndex(options);

    log.info("Adding step 1: Options");
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(hdfsSiteConfig, options.getProperties());

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    MetadataTransform metadataTransform = MetadataTransform.create(properties, endPointType, attempt, skipRegisrtyCalls);
    BasicTransform basicTransform = BasicTransform.create(properties, datasetId, tripletValid, occurrenceIdValid, useExtendedRecordId);
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create(properties);
    LocationTransform locationTransform = LocationTransform.create(properties);
    TaggedValuesTransform taggedValuesTransform = TaggedValuesTransform.create();
    // Extension
    MeasurementOrFactTransform measurementOrFactTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    log.info("Reading xml files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", XmlIO.read(options.getInputPath()))
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Filter duplicates", UniqueIdTransform.create())
            .apply("Set default values", DefaultValuesTransform.create(properties, datasetId, skipRegisrtyCalls));

    log.info("Adding step 3: Reading avros");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create metadata collection", Create.of(datasetId))
            .apply("Interpret metadata", metadataTransform.interpret())
            .apply("Convert into view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        uniqueRecords
          .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, TaggedValueRecord>> taggedValuesCollection =
      uniqueRecords
        .apply("Interpret TaggedValueRecords/MachinesTags interpretation", taggedValuesTransform.interpret(metadataView))
        .apply("Map TaggedValueRecord to KV", taggedValuesTransform.toKv());

    // Core
    PCollection<KV<String, BasicRecord>> basicCollection =
        uniqueRecords
            .apply("Interpret basic", basicTransform.interpret())
            .apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        uniqueRecords
            .apply("Interpret temporal", temporalTransform.interpret())
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        uniqueRecords
            .apply("Interpret location", locationTransform.interpret(metadataView))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        uniqueRecords
            .apply("Interpret taxonomy", taxonomyTransform.interpret())
            .apply("Map Taxon to KV", taxonomyTransform.toKv());

    // Extension
    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        uniqueRecords
            .apply("Interpret multimedia", multimediaTransform.interpret())
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        uniqueRecords
            .apply("Interpret image", imageTransform.interpret())
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        uniqueRecords
            .apply("Interpret audubon", audubonTransform.interpret())
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection =
        uniqueRecords
            .apply("Interpret measurement", measurementOrFactTransform.interpret())
            .apply("Map MeasurementOrFact to KV", measurementOrFactTransform.toKv());

    log.info("Adding step 4: Group and convert object into a json");
    SingleOutput<KV<String, CoGbkResult>, String> gbifJsonDoFn =
        GbifJsonTransform.create(
            verbatimTransform.getTag(), basicTransform.getTag(), temporalTransform.getTag(), locationTransform.getTag(),
            taxonomyTransform.getTag(),  multimediaTransform.getTag(), imageTransform.getTag(), audubonTransform.getTag(),
            measurementOrFactTransform.getTag(),taggedValuesTransform.getTag(), metadataView
        ).converter();

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(taxonomyTransform.getTag(), taxonCollection)
            .and(taggedValuesTransform.getTag(), taggedValuesCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            .and(measurementOrFactTransform.getTag(), measurementCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", gbifJsonDoFn);

    log.info("Adding step 5: Elasticsearch indexing");
    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), Indexing.INDEX_TYPE);

    jsonCollection.apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withMaxBatchSize(options.getEsMaxBatchSize())
            .withIdFn(input -> input.get(GBIF_ID).asText()));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    EsIndexUtils.swapIndexIfAliasExists(options, LockConfigFactory.create(properties, PipelinesVariables.Lock.ES_LOCK_PREFIX));

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");

    FsUtils.removeTmpDirectory(options);
  }
}
