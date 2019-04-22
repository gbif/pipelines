package org.gbif.pipelines.ingest.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
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
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *      {@link org.gbif.pipelines.io.avro.AustraliaSpatialRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline
 * --properties=/some/path/to/output/ws.properties
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --interpretationTypes=ALL
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    String datasetId = options.getDatasetId();
    String attempt = options.getAttempt().toString();

    FsUtils.deleteInterpretIfExist(options.getHdfsSiteConfig(), datasetId, attempt, options.getInterpretationTypes());

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt);

    List<String> types = options.getInterpretationTypes();
    String propertiesPath = options.getProperties();
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpret(options, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    log.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", VerbatimTransform.read(options.getInputPath()))
            .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
            .apply("Hash ID", HashIdTransform.create(datasetId))
            .apply("Filter duplicates", UniqueIdTransform.create());

    log.info("Adding interpretations");

    //Create metadata
    PCollection<MetadataRecord> metadataRecordP =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", MetadataTransform.interpret(propertiesPath, options.getEndPointType()));

    //Write metadata
    metadataRecordP.apply("Write metadata to avro", MetadataTransform.write(pathFn));

    //Create View for further use
    PCollectionView<MetadataRecord> metadataView = metadataRecordP.apply("Convert into view", View.asSingleton());

    uniqueRecords
        .apply("Check verbatim transform condition", VerbatimTransform.check(types))
        .apply("Write verbatim to avro", VerbatimTransform.write(pathFn));

    uniqueRecords
        .apply("Check basic transform condition", BasicTransform.check(types))
        .apply("Interpret basic", BasicTransform.interpret(propertiesPath, datasetId))
        .apply("Write basic to avro", BasicTransform.write(pathFn));

    uniqueRecords
        .apply("Check temporal transform condition", TemporalTransform.check(types))
        .apply("Interpret temporal", TemporalTransform.interpret())
        .apply("Write temporal to avro", TemporalTransform.write(pathFn));

    uniqueRecords
        .apply("Check multimedia transform condition", MultimediaTransform.check(types))
        .apply("Interpret multimedia", MultimediaTransform.interpret())
        .apply("Write multimedia to avro", MultimediaTransform.write(pathFn));

    uniqueRecords
        .apply("Check image transform condition", ImageTransform.check(types))
        .apply("Interpret image", ImageTransform.interpret())
        .apply("Write image to avro", ImageTransform.write(pathFn));

    uniqueRecords
        .apply("Check audubon transform condition", AudubonTransform.check(types))
        .apply("Interpret audubon", AudubonTransform.interpret())
        .apply("Write audubon to avro", AudubonTransform.write(pathFn));

    uniqueRecords
        .apply("Check measurement transform condition", MeasurementOrFactTransform.check(types))
        .apply("Interpret measurement", MeasurementOrFactTransform.interpret())
        .apply("Write measurement to avro", MeasurementOrFactTransform.write(pathFn));

    uniqueRecords
        .apply("Check taxonomy transform condition", TaxonomyTransform.check(types))
        .apply("Interpret taxonomy", TaxonomyTransform.interpret(propertiesPath))
        .apply("Write taxon to avro", TaxonomyTransform.write(pathFn));

    uniqueRecords
        .apply("Check location transform condition", LocationTransform.check(types))
        .apply("Interpret location", LocationTransform.interpret(propertiesPath, metadataView))
        .apply("Write location to avro", LocationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile(options.getHdfsSiteConfig(), metadataPath, result);
    });

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", options.getTargetPath(), datasetId, attempt);
    FsUtils.deleteDirectoryByPrefix(options.getHdfsSiteConfig(), tempPath, ".temp-beam");

    log.info("Pipeline has been finished");
  }
}
