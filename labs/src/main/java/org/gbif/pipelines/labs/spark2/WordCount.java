package org.gbif.pipelines.labs.spark2;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sample program demonstrates running beam pipeline as spark job without spark submit
 */
public class WordCount {

  private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

  public static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);
    p.apply("Reading file", TextIO.read().from(options.getInputFile()))
      .apply(ParDo.of(new WordCountFn()))
      .apply(Sum.integersGlobally())
      .apply(MapElements.into(TypeDescriptor.of(String.class)).via((val) -> val + ""))
      .apply(TextIO.write().to(options.getOutputFile()));
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  public static void main(String[] args) {
    WordCountOptions as = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
    as.setRunner(SparkRunner.class);
    runWordCount(as);

  }

  static interface WordCountOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    String getOutputFile();

    void setOutputFile(String value);
  }

  static class WordCountFn extends DoFn<String, Integer> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element().split("[^\\p{L}]+").length);
    }
  }

}
