package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.HashIdTransform;
import org.gbif.pipelines.transforms.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
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
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToInterpretedPipeline
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
    String endPointType = options.getEndPointType();
    boolean tripletValid = options.isTripletValid();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", options.getAttempt().toString());

    String propertiesPath = options.getProperties();
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpret(options, t, id);

    String inputPath = options.getInputPath();
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    String tmpDir = FsUtils.getTempDir(options);

    DwcaIO.Read reader = isDir ? DwcaIO.Read.fromLocation(inputPath) : DwcaIO.Read.fromCompressed(inputPath, tmpDir);

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    log.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Hash ID", HashIdTransform.create(datasetId))
            .apply("Filter duplicates", UniqueIdTransform.create());

    log.info("Adding interpretations");

    //Create metadata
    PCollection<MetadataRecord> metadataRecords =
        p.apply("Create metadata collection", Create.of(datasetId))
            .apply("Interpret metadata", MetadataTransform.interpret(propertiesPath, endPointType));

    //Write metadata
    metadataRecords.apply("Write metadata to avro", MetadataTransform.write(pathFn));

    //Create View for further use
    PCollectionView<MetadataRecord> metadataView = metadataRecords.apply("Convert into view", View.asSingleton());

    uniqueRecords
        .apply("Write unique verbatim to avro", VerbatimTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret basic", BasicTransform.interpret(propertiesPath, datasetId, tripletValid, occurrenceIdValid))
        .apply("Write basic to avro", BasicTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret temporal", TemporalTransform.interpret())
        .apply("Write temporal to avro", TemporalTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret multimedia", MultimediaTransform.interpret())
        .apply("Write multimedia to avro", MultimediaTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret image", ImageTransform.interpret())
        .apply("Write image to avro", ImageTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret audubon", AudubonTransform.interpret())
        .apply("Write audubon to avro", AudubonTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret measurement", MeasurementOrFactTransform.interpret())
        .apply("Write measurement to avro", MeasurementOrFactTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret taxonomy", TaxonomyTransform.interpret(propertiesPath))
        .apply("Write taxon to avro", TaxonomyTransform.write(pathFn));

    uniqueRecords
        .apply("Interpret location", LocationTransform.interpret(propertiesPath, metadataView))
        .apply("Write location to avro", LocationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile(options.getHdfsSiteConfig(), metadataPath, result);
    });

    log.info("Pipeline has been finished");
  }
}
