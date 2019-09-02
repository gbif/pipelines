package org.gbif.pipelines.ingest.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables.Lock;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView;
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
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.config.LockConfig;
import org.gbif.pipelines.parsers.config.LockConfigFactory;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MetadataTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.hdfs.FilterMissedGbifIdTransform;
import org.gbif.pipelines.transforms.hdfs.OccurrenceHdfsRecordConverterTransform;
import org.gbif.pipelines.transforms.hdfs.OccurrenceHdfsRecordTransform;

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
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;
import static org.gbif.pipelines.ingest.utils.FsUtils.buildPathHdfsViewUsingInputPath;

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
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToHiveViewPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.base.pipelines.InterpretedToHiveViewPipeline
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=hdfs://ha-nn/output/
 * --esIndexName=pipeline
 * --hdfsSiteConfig=/config/hdfs-site.xml
 * --coreSiteConfig=/config/core-site.xml
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
    Set<String> types = Collections.singleton(OCCURRENCE_HDFS_RECORD.name());
    String targetTempPath = buildPathHdfsViewUsingInputPath(options, datasetId + '_' + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());

    //Deletes the target path if it exists
    FsUtils.deleteInterpretIfExist(hdfsSiteConfig, options.getInputPath(), datasetId, attempt, types);

    log.info("Adding step 1: Options");
    UnaryOperator<String> interpretPathFn = t -> FsUtils.buildPathInterpretUsingInputPath(options, t, "*" + AVRO_EXTENSION);

    // Core
    final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
    final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
    final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
    final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
    final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
    // Extension
    final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};
    final TupleTag<ImageRecord> irTag = new TupleTag<ImageRecord>() {};
    final TupleTag<AudubonRecord> arTag = new TupleTag<AudubonRecord>() {};
    final TupleTag<MeasurementOrFactRecord> mfrTag = new TupleTag<MeasurementOrFactRecord>() {};

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Reading avros");
    // Core
    BasicTransform basicTransform = BasicTransform.create();
    MetadataTransform metadataTransform = MetadataTransform.create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create();
    LocationTransform locationTransform = LocationTransform.create();
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
        OccurrenceHdfsRecordConverterTransform.create(erTag, brTag, trTag, lrTag, txrTag, mrTag, irTag, arTag, mfrTag, metadataView)
            .converter();

    PCollection<OccurrenceHdfsRecord> hdfsRecordPCollection =
        KeyedPCollectionTuple
            // Core
            .of(brTag, basicCollection)
            .and(trTag, temporalCollection)
            .and(lrTag, locationCollection)
            .and(txrTag, taxonCollection)
            // Extension
            .and(mrTag, multimediaCollection)
            .and(irTag, imageCollection)
            .and(arTag, audubonCollection)
            .and(mfrTag, measurementCollection)
            // Raw
            .and(erTag, verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to HdfsRecord", toHdfsRecordDoFn)
            .apply("Removing records with invalid gbif ids", FilterMissedGbifIdTransform.create());

    hdfsRecordPCollection.apply(OccurrenceHdfsRecordTransform.create().write(targetTempPath));

    log.info("Running the pipeline");
    PipelineResult result = p.run();

    if (PipelineResult.State.DONE == result.waitUntilFinish()) {
      //A write lock is acquired to avoid concurrent modifications while this operation is running
      Properties properties = FsUtils.readPropertiesFile(hdfsSiteConfig, options.getProperties());
      LockConfig lockConfig = LockConfigFactory.create(properties, Lock.HDFS_LOCK_PREFIX);
      SharedLockUtils.doInBarrier(lockConfig, () -> copyOccurrenceRecords(options));
    }

    //Metrics
    MetricsHandler.saveCountersToInputPathFile(options, result);

    log.info("Pipeline has been finished");
  }

  /**
   * Copies all occurrence records into the directory from targetPath.
   * Deletes pre-existing data of the dataset being processed.
   */
  private static void copyOccurrenceRecords(InterpretationPipelineOptions options) {
    //Moving files to the directory of latest records
    String targetPath = options.getTargetPath();

    String deletePath = FsUtils.buildPath(targetPath, HdfsView.VIEW_OCCURRENCE + "_" + options.getDatasetId() + "_*").toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(options.getHdfsSiteConfig(), deletePath);
    String filter = buildPathHdfsViewUsingInputPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filter, targetPath);
    FsUtils.moveDirectory(options.getHdfsSiteConfig(), filter, targetPath);
    log.info("Files moved to {} directory", targetPath);
  }
}
