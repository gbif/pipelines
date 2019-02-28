package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.util.Optional;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.transforms.core.VerbatimTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.MDC;

import lombok.extern.slf4j.Slf4j;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads DwCA archive and converts to {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 *    2) Writes data to verbatim.avro file
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82
 * --attempt=1
 * --runner=DirectRunner
 * --targetPath=/path/GBIF/output/
 * --inputPath=/path/GBIF/input/dwca/9f747cff-839f-4485-83a1-f10317a92a82.dwca
 *
 * }</pre>
 */
@Slf4j
public class DwcaToVerbatimPipeline {

  private DwcaToVerbatimPipeline() {}

  public static void main(String[] args) {
    BasePipelineOptions options = PipelinesOptionsFactory.create(BasePipelineOptions.class, args);
    run(options);
  }

  public static void run(BasePipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();
    String targetPath = FsUtils.buildPath(options, Conversion.FILE_NAME);
    String tmpPath = FsUtils.getTempDir(options);

    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    DwcaIO.Read reader = isDir ? DwcaIO.Read.fromLocation(inputPath) : DwcaIO.Read.fromCompressed(inputPath, tmpPath);

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = Pipeline.create(options);

    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", VerbatimTransform.write(targetPath).withoutSharding());

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile("", metadataPath, result);
    });

    log.info("Pipeline has been finished");
  }
}
