package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.BasePipelineOptions;
import org.gbif.pipelines.base.transforms.WriteTransforms;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.common.beam.DwcaIO;

import java.nio.file.Paths;

import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwcaToAvroPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroPipeline.class);

  private final BasePipelineOptions options;

  private DwcaToAvroPipeline(BasePipelineOptions options) {
    this.options = options;
  }

  public static DwcaToAvroPipeline create(BasePipelineOptions options) {
    return new DwcaToAvroPipeline(options);
  }

  /** TODO: DOC! */
  public static void main(String[] args) {
    BasePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(BasePipelineOptions.class);
    DwcaToAvroPipeline.create(options).run();
  }

  /** TODO: DOC! */
  public PipelineResult.State run() {

    String inputPath = options.getInputPath();
    String targetPath = options.getTargetPath();
    boolean isDirectory = Paths.get(inputPath).toFile().isDirectory();

    String tmp =
        Strings.isNullOrEmpty(options.getTempLocation())
            ? FsUtils.buildPathString(options.getTargetPath(), "tmp")
            : options.getTempLocation();

    DwcaIO.Read reader =
        isDirectory ? DwcaIO.Read.withPaths(inputPath) : DwcaIO.Read.withPaths(inputPath, tmp);

    Pipeline p = Pipeline.create(options);

    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", WriteTransforms.extended(targetPath));

    LOG.info("Running the pipeline");
    return p.run().waitUntilFinish();
  }
}
