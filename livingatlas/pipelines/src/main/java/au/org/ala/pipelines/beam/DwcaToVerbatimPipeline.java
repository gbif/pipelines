package au.org.ala.pipelines.beam;

import java.nio.file.Paths;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.beam.DwcaExtendedRecordIO;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.slf4j.MDC;

/**
 * Pipeline that reads a DwCA an converts to AVRO using the {@link
 * org.gbif.pipelines.io.avro.ExtendedRecord}
 *
 * <p>Pipeline sequence:
 *
 * <pre>
 *    1) Reads DwCA archive and converts to {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 *    2) Writes data to verbatim.avro file
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
 *  --pipelineStep=DWCA_TO_VERBATIM \
 * --datasetId=9f747cff-839f-4485-83a1-f10317a92a82
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=/path/GBIF/output/
 * --inputPath=/path/GBIF/input/dwca/9f747cff-839f-4485-83a1-f10317a92a82.dwca
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaToVerbatimPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();
    String targetPath = PathBuilder.buildDatasetAttemptPath(options, Conversion.FILE_NAME, false);
    String tmpPath = PathBuilder.getTempDir(options);

    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    DwcaExtendedRecordIO.Read reader =
        isDir
            ? DwcaExtendedRecordIO.Read.fromLocation(inputPath)
            : DwcaExtendedRecordIO.Read.fromCompressed(inputPath, tmpPath);

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = Pipeline.create(options);

    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", VerbatimTransform.create().write(targetPath).withoutSharding());

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
