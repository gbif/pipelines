package org.gbif.pipelines.examples;

import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Example of using Apache Beam Metrics for producing various kinds of metrics for reporting
 * properties of an executing pipeline.
 *
 * <p>You can run pipeline (Please change BUILD_VERSION to the current project version):
 *
 * <pre>{@code
 * java -jar target/examples-metrics-BUILD_VERSION-shaded.jar src/main/resources/example.properties
 *
 * or pass all parameters:
 *
 * java -jar target/examples-metrics-BUILD_VERSION-shaded.jar --runner=SparkRunner --inputPath=foobar.txt
 *
 * }</pre>
 */
public class MetricsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsPipeline.class);

  public static void main(String[] args) {

    // Passing custom value to the logger
    MDC.put("uuid", UUID.randomUUID().toString());

    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, args);
    Pipeline p = Pipeline.create(options);

    p.apply("Reads file", TextIO.read().from(options.getInputPath()))
        .apply("Filters words", ParDo.of(new FilterTextFn()));

    LOG.info("Running the pipeline");
    p.run().waitUntilFinish();
    LOG.info("Pipeline has been finished");
  }

  /**
   * Simple DoFn filters non-foo words. Using {@link Counter} we can count how many records were
   * filtered
   */
  private static class FilterTextFn extends DoFn<String, String> {

    private final Counter fooCounter = Metrics.counter(MetricsPipeline.class, "foo");
    private final Counter nonfooCounter = Metrics.counter(MetricsPipeline.class, "nonfoo");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String element = c.element();
      if (element.equals("foo")) {
        fooCounter.inc();
        c.output(element);
      } else {
        nonfooCounter.inc();
      }
    }
  }
}
