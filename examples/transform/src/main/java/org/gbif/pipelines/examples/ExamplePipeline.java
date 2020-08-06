package org.gbif.pipelines.examples;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of using @see <a href="https://beam.apache.org/contribute/runner-guide/">Apache Beam</a>
 * pipeline.
 *
 * <p>You can run pipeline (Please change BUILD_VERSION to the current project version):
 *
 * <pre>{@code
 * java -jar target/examples-transform-BUILD_VERSION-shaded.jar src/main/resources/example.properties
 *
 * or pass all parameters:
 *
 * java -jar target/examples-transform-BUILD_VERSION-shaded.jar --runner=DirectRunner --targetPath=target/example-record --inputPath=src/main/resources/example.zip
 *
 * }</pre>
 */
public class ExamplePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ExamplePipeline.class);

  public static void main(String[] args) {

    // Reads input arguments or arguments from file
    BasePipelineOptions options = PipelinesOptionsFactory.create(BasePipelineOptions.class, args);

    // Pipeline properties
    String inputPath = options.getInputPath(); // Path to DwCA zip archive
    String tmpDir = FsUtils.getTempDir(options); // Path to temporal directory or creates new
    String outPath = options.getTargetPath(); // Path to output *.avro files

    // Creates pipeline from options
    Pipeline p = Pipeline.create(options);

    // Reads DwCA archive and convert to ExtendedRecord
    p.apply("Read DwCA zip archive", DwcaIO.Read.fromCompressed(inputPath, tmpDir))
        // Interprets and transforms from ExtendedRecord to TemporalRecord using GBIF
        // TemporalInterpreter
        .apply("Interpret TemporalRecord", TemporalTransform.create().interpret())
        // Interprets and Transforms from ExtendedRecord to ExampleRecord using ExampleInterpreter
        .apply("Interpret ExampleRecord", ExampleTransform.exampleOne())
        // Write ExampleRecords as Avro files using AvroIO.Write
        .apply(
            "Write as Avro files",
            AvroIO.write(ExampleRecord.class).to(outPath).withSuffix(AVRO_EXTENSION));

    LOG.info("Running the pipeline");
    p.run().waitUntilFinish();
    LOG.info("Pipeline has been finished!");
  }
}
