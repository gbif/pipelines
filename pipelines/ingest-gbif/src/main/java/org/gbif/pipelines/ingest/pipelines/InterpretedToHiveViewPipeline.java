package org.gbif.pipelines.ingest.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Lock;
import org.gbif.pipelines.ingest.hdfs.converters.FilterMissedGbifIdTransform;
import org.gbif.pipelines.ingest.hdfs.converters.OccurrenceHdfsRecordTransform;
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
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200
 * --hdfsSiteConfig=/config/hdfs-site.xml
 * --coreSiteConfig=/config/core-site.xml
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToHiveViewPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  /**
   * Builds the target path based on a file name or else in file selector.
   * @param options pipeline options
   * @param fileSelector pattern of target name or file selector
   * @return path to the target file or pattern
   */
  private static String targetPath(InterpretationPipelineOptions options, String fileSelector) {
    return FsUtils.buildPath(occurrenceHdfsViewTargetPath(options),
                            "view_occurrence" + fileSelector)
                            .toString();
  }

  /**
   * Builds the target base path of the Occurrence hdfs view.
   * @param options options pipeline options
   * @return path to the directory where the occurrence hdfs view is stored
   */
  private static String occurrenceHdfsViewTargetPath(InterpretationPipelineOptions options) {
    return FsUtils.buildPath(options.getTargetPath(),
                            options.getDatasetId(),
                            options.getAttempt().toString(),
                            PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME,
                            OccurrenceHdfsRecord.class.getSimpleName().toLowerCase())
                            .toString();
  }

  public static void run(InterpretationPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    String id = options.getDatasetId() + '_' + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    String targetPath = targetPath(options, id);

    //Deletes the target path if it exists
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), occurrenceHdfsViewTargetPath(options));

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpret(options, t, "*" + AVRO_EXTENSION);

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

    PCollection<KV<String, TaxonRecord>> taxonCollection =
      p.apply("Read Taxon", taxonomyTransform.read(pathFn))
        .apply("Map Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
      p.apply("Read Multimedia", multimediaTransform.read(pathFn))
        .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
      p.apply("Read Image", imageTransform.read(pathFn))
        .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
      p.apply("Read Audubon", audubonTransform.read(pathFn))
        .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection =
      p.apply("Read Measurement", measurementOrFactTransform.read(pathFn))
        .apply("Map Measurement to KV", measurementOrFactTransform.toKv());

    log.info("Adding step 3: Converting into a OccurrenceHdfsRecord object");
    SingleOutput<KV<String, CoGbkResult>, OccurrenceHdfsRecord> toHdfsRecordDoFn =
        OccurrenceHdfsRecordTransform.Transform.create(erTag, brTag, trTag, lrTag, txrTag, mrTag, irTag, arTag, mfrTag, metadataView)
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


    hdfsRecordPCollection.apply(OccurrenceHdfsRecordTransform.write(targetPath));

    log.info("Running the pipeline");
    PipelineResult result = p.run();

    if (PipelineResult.State.DONE == result.waitUntilFinish()) {
      //A write lock is acquired to avoid concurrent modifications while this operation is running
      Properties properties = FsUtils.readPropertiesFile(options.getHdfsSiteConfig(), options.getProperties());
      LockConfig lockConfig = LockConfigFactory.create(properties, Lock.HDFS_LOCK_PREFIX);
      SharedLockUtils.doInBarrier(lockConfig, () -> copyOccurrenceRecords(options));
    }

    //Metrics
    MetricsHandler.saveCountersToFile(options, result);

    log.info("Pipeline has been finished");
  }


  /**
   * Copies all occurrence records into the "hdfsview/occurrence" directory.
   * Deletes pre-existing data of the dataset being processed.
   */
  private static void copyOccurrenceRecords(InterpretationPipelineOptions options) {
    log.info("Copying avro files to hdfsview/occurrence");
    //Moving files to the directory of latest records
    String occurrenceHdfsViewPath = FsUtils.buildPath(options.getTargetPath(), "hdfsview/occurrence").toString();

    FsUtils.deleteByPattern(options.getHdfsSiteConfig(), occurrenceHdfsViewPath + "/*" + options.getDatasetId() + '*');
    String filter = targetPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filter, occurrenceHdfsViewPath);
    FsUtils.moveDirectory(options.getHdfsSiteConfig(), filter, occurrenceHdfsViewPath);
    log.info("Files moved to hdfsview/occurrnce directory");
  }
}
