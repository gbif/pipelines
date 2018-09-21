package org.gbif.pipelines.examples;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.transforms.RecordTransforms;
import org.gbif.pipelines.transforms.WriteTransforms;

import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of using @see <a href="https://beam.apache.org/contribute/runner-guide/">Apache Beam</a>
 * pipeline.
 *
 * <p>You can run pipeline (Please change BUILD_VERSION to the current project version):
 *
 * <pre>{@code
 * java -cp target/examples-BUILD_VERSION-shaded.jar org.gbif.pipelines.examples.ExamplePipeline src/main/resources/example.properties
 *
 * or pass all parameters:
 *
 * java -cp target/examples-BUILD_VERSION-shaded.jar org.gbif.pipelines.examples.ExamplePipeline --runner=DirectRunner --targetPath=target/example-record --inputPath=example.zip
 *
 * }</pre>
 */
public class ExamplePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ExamplePipeline.class);

  public static void main(String[] args) {

    // Reads input arguments or arguments from file
    BasePipelineOptions options = PipelinesOptionsFactory.create(BasePipelineOptions.class, args);

    // Pipeline properties
    String inputPath = options.getInputPath(); // Path to DwCA zip arhive
    String tmpDir = FsUtils.getTempDir(options); // Path to temporal directory or creates new
    String outPath = options.getTargetPath(); // Path to output *.avro files

    // Creates pipeline from options
    Pipeline p = Pipeline.create(options);

    // Reads DwCA archive and convert to ExtendedRecord
    p.apply("Read DwCA zip archive", DwcaIO.Read.fromCompressed(inputPath, tmpDir))
        // Interprets and transforms from ExtendedRecord to TemporalRerord using GBIF TemporalInterpreter
        .apply("Interpret TemporalRerord", RecordTransforms.temporal())
        // Interprets and Transforms from ExtendedRecord to ExampleRecord using ExampleInterpreter
        .apply("Intertret ExampleRecord", ExampleTransform.exampleOne())
        // Write ExampleRecords as avro files using AvroIO.Write
        .apply("Write as avro files", WriteTransforms.create(ExampleRecord.class, outPath));

    LOG.info("Running the pipeline");
    p.run().waitUntilFinish();
    LOG.info("Pipeline has been finished!");
  }
}
