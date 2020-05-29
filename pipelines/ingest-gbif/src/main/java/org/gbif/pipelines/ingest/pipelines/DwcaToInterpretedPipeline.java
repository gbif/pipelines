package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads DwCA archive and converts to {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 *  --pipelineStep=DWCA_TO_INTERPRETED \
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/input/dwca/dwca.zip
 * --runner=SparkRunner
 * --properties=/path/ws.properties
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaToInterpretedPipeline {

  public static void main(String[] args) {
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);
    run(options);
  }

  public static void run(DwcaPipelineOptions options) {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    String endPointType = options.getEndPointType();
    boolean tripletValid = options.isTripletValid();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();
    boolean useExtendedRecordId = options.isUseExtendedRecordId();
    boolean skipRegistryCalls = options.isSkipRegisrtyCalls();

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());

    String hdfsSiteConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(hdfsSiteConfig, options.getProperties());
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, id);

    String inputPath = options.getInputPath();
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    String tmpDir = FsUtils.getTempDir(options);

    DwcaIO.Read reader = isDir ? DwcaIO.Read.fromLocation(inputPath) : DwcaIO.Read.fromCompressed(inputPath, tmpDir);

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    log.info("Creating transformations");
    // Core
    MetadataTransform metadataTransform = MetadataTransform.create(properties, endPointType, attempt, skipRegistryCalls);
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

    log.info("Adding step 3: Creating beam pipeline");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Filter duplicates", UniqueIdTransform.create());

    //Create metadata
    PCollection<MetadataRecord> metadataRecords =
        p.apply("Create metadata collection", Create.of(datasetId))
            .apply("Interpret metadata", metadataTransform.interpret());

    //Write metadata
    metadataRecords.apply("Write metadata to avro", metadataTransform.write(pathFn));

    //Create View for further use
    PCollectionView<MetadataRecord> metadataView = metadataRecords.apply("Convert into view", View.asSingleton());

    uniqueRecords
        .apply("Write unique verbatim to avro", verbatimTransform.write(pathFn));

    uniqueRecords
       .apply("Interpret TaggedValueRecords/MachinesTags interpretation", taggedValuesTransform.interpret(metadataView))
       .apply("Map TaggedValueRecord to KV", taggedValuesTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret basic", basicTransform.interpret())
        .apply("Write basic to avro", basicTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret temporal", temporalTransform.interpret())
        .apply("Write temporal to avro", temporalTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret multimedia", multimediaTransform.interpret())
        .apply("Write multimedia to avro", multimediaTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret image", imageTransform.interpret())
        .apply("Write image to avro", imageTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret audubon", audubonTransform.interpret())
        .apply("Write audubon to avro", audubonTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret measurement", measurementOrFactTransform.interpret())
        .apply("Write measurement to avro", measurementOrFactTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret taxonomy", taxonomyTransform.interpret())
        .apply("Write taxon to avro", taxonomyTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret location", locationTransform.interpret(metadataView))
        .apply("Write location to avro", locationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
