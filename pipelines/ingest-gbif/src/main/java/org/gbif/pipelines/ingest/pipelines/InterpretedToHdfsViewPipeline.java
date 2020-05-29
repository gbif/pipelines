package org.gbif.pipelines.ingest.pipelines;

import java.util.Collections;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.ingest.utils.SharedLockUtils;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.common.OccurrenceHdfsRecordTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceHdfsRecordConverterTransform;
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

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;
import static org.gbif.pipelines.ingest.utils.FsUtils.buildFilePathHdfsViewUsingInputPath;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link MetadataRecord},
 *      {@link BasicRecord},
 *      {@link TemporalRecord},
 *      {@link MultimediaRecord},
 *      {@link ImageRecord},
 *      {@link AudubonRecord},
 *      {@link MeasurementOrFactRecord},
 *      {@link TaxonRecord},
 *      {@link LocationRecord}
 *    2) Joins avro files
 *    3) Converts to a {@link OccurrenceHdfsRecord} based on the input files
 *    4) Moves the produced files to a directory where the latest version of HDFS records are kept
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=INTERPRETED_TO_HDFS \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --runner=SparkRunner \
 * --inputPath=/path \
 * --targetPath=/path \
 * --properties=/path/pipelines.properties
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToHdfsViewPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Integer numberOfShards = options.getNumberOfShards();
    Set<String> types = Collections.singleton(OCCURRENCE_HDFS_RECORD.name());
    String targetTempPath = buildFilePathHdfsViewUsingInputPath(options, datasetId + '_' + attempt);

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.HDFS_VIEW.name());

    //Deletes the target path if it exists
    FsUtils.deleteInterpretIfExist(hdfsSiteConfig, options.getInputPath(), datasetId, attempt, types);

    log.info("Adding step 1: Options");
    UnaryOperator<String> interpretPathFn = t -> FsUtils.buildPathInterpretUsingInputPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Reading AVROs");
    // Core
    BasicTransform basicTransform = BasicTransform.create();
    MetadataTransform metadataTransform = MetadataTransform.create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create();
    LocationTransform locationTransform = LocationTransform.create();
    TaggedValuesTransform taggedValuesTransform = TaggedValuesTransform.create();
    // Extension
    MeasurementOrFactTransform measurementOrFactTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(interpretPathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(interpretPathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, TaggedValueRecord>> taggedValuesCollection =
      p.apply("Interpret TaggedValueRecords/MachinesTags interpretation", taggedValuesTransform.read(interpretPathFn))
        .apply("Map TaggedValueRecord to KV", taggedValuesTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", basicTransform.read(interpretPathFn))
            .apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(interpretPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(interpretPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", taxonomyTransform.read(interpretPathFn))
            .apply("Map Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(interpretPathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", imageTransform.read(interpretPathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", audubonTransform.read(interpretPathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection =
        p.apply("Read Measurement", measurementOrFactTransform.read(interpretPathFn))
            .apply("Map Measurement to KV", measurementOrFactTransform.toKv());

    log.info("Adding step 3: Converting into a OccurrenceHdfsRecord object");
    SingleOutput<KV<String, CoGbkResult>, OccurrenceHdfsRecord> toHdfsRecordDoFn =
        OccurrenceHdfsRecordConverterTransform.create(
            verbatimTransform.getTag(), basicTransform.getTag(), temporalTransform.getTag(), locationTransform.getTag(),
            taxonomyTransform.getTag(),  multimediaTransform.getTag(), imageTransform.getTag(), audubonTransform.getTag(),
            measurementOrFactTransform.getTag(), taggedValuesTransform.getTag(), metadataView
        ).converter();

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
        .apply("Merging to HdfsRecord", toHdfsRecordDoFn)
        .apply(OccurrenceHdfsRecordTransform.create().write(targetTempPath, numberOfShards));

    log.info("Running the pipeline");
    PipelineResult result = p.run();

    if (PipelineResult.State.DONE == result.waitUntilFinish()) {
      SharedLockUtils.doHdfsPrefixLock(options, () -> FsUtils.copyOccurrenceRecords(options));
    }

    //Metrics
    MetricsHandler.saveCountersToInputPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

}
