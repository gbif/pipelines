package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.Function;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.config.KvConfig;
import org.gbif.pipelines.parsers.config.KvConfigFactory;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;
import org.gbif.pipelines.transforms.UniqueIdTransform;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.MDC;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUDUBON;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IMAGE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TEMPORAL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.VERBATIM;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads DwCA archive and converts to {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link  org.gbif.pipelines.io.avro.TaxonRecord},
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
 *
 * }</pre>
 */
@Slf4j
public class DwcaToInterpretedPipeline {

  private DwcaToInterpretedPipeline() {}

  public static void main(String[] args) {
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);
    run(options);
  }

  public static void run(DwcaPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    WsConfig wsConfig = WsConfigFactory.create(options.getGbifApiUrl());
    KvConfig kvConfig = KvConfigFactory.create(options.getGbifApiUrl(), options.getZookeeperUrl());
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    Function<RecordType, String> pathFn = t -> FsUtils.buildPathInterpret(options, t.name(), id);

    String inputPath = options.getInputPath();
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    String tmpDir = FsUtils.getTempDir(options);

    DwcaIO.Read reader = isDir ? DwcaIO.Read.fromLocation(inputPath) : DwcaIO.Read.fromCompressed(inputPath, tmpDir);

    log.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    log.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Filter duplicates", UniqueIdTransform.create());

    log.info("Adding interpretations");

    p.apply("Create metadata collection", Create.of(options.getDatasetId()))
        .apply("Interpret metadata", ParDo.of(new MetadataTransform.Interpreter(wsConfig)))
        .apply("Write metadata to avro", MetadataTransform.write(pathFn.apply(METADATA)));

    uniqueRecords
        .apply("Write unique verbatim to avro", VerbatimTransform.write(pathFn.apply(VERBATIM)));

    uniqueRecords
        .apply("Interpret basic", ParDo.of(new BasicTransform.Interpreter()))
        .apply("Write basic to avro", BasicTransform.write(pathFn.apply(BASIC)));

    uniqueRecords
        .apply("Interpret temporal", ParDo.of(new TemporalTransform.Interpreter()))
        .apply("Write temporal to avro", TemporalTransform.write(pathFn.apply(TEMPORAL)));

    uniqueRecords
        .apply("Interpret multimedia", ParDo.of(new MultimediaTransform.Interpreter()))
        .apply("Write multimedia to avro", MultimediaTransform.write(pathFn.apply(MULTIMEDIA)));

    uniqueRecords
        .apply("Interpret image", ParDo.of(new ImageTransform.Interpreter()))
        .apply("Write image to avro", ImageTransform.write(pathFn.apply(IMAGE)));

    uniqueRecords
        .apply("Interpret audubon", ParDo.of(new AudubonTransform.Interpreter()))
        .apply("Write audubon to avro", AudubonTransform.write(pathFn.apply(AUDUBON)));

    uniqueRecords
        .apply("Interpret measurement", ParDo.of(new MeasurementOrFactTransform.Interpreter()))
        .apply("Write measurement to avro", MeasurementOrFactTransform.write(pathFn.apply(MEASUREMENT_OR_FACT)));

    uniqueRecords
        .apply("Interpret taxonomy", ParDo.of(new TaxonomyTransform.Interpreter(kvConfig)))
        .apply("Write taxon to avro", TaxonomyTransform.write(pathFn.apply(TAXONOMY)));

    uniqueRecords
        .apply("Interpret location", ParDo.of(new LocationTransform.Interpreter(kvConfig)))
        .apply("Write location to avro", LocationTransform.write(pathFn.apply(LOCATION)));

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
