package org.gbif.pipelines.ingest.pipelines;

import java.nio.file.Paths;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.Transform;

import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FragmenterPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.DWCA_TO_VERBATIM.name());

    log.info("Adding step 1: Options");
    String inputPath = options.getInputPath();
    String tmpPath = PathBuilder.getTempDir(options);

    boolean isDir = Paths.get(inputPath).toFile().isDirectory();

    DwcaIO.Read reader =
        isDir
            ? DwcaIO.Read.fromLocation(inputPath)
            : DwcaIO.Read.fromCompressed(inputPath, tmpPath);

    log.info("Adding step 2: Pipeline steps");
    Pipeline p = pipelinesFn.apply(options);

    //    p.apply("Read from Darwin Core Archive", reader)
    //        .apply("Filter existing fragments", HBaseIO.read().withTableId("").withScan(new
    // Scan()))
    //        .apply("Push fragments to HBase", null);

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  public static class RecordFilter extends Transform<DwcaOccurrenceRecord, DwcaOccurrenceRecord> {

  }
}
