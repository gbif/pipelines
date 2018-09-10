package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.BasePipelineOptions;
import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.transforms.WriteTransforms;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.common.beam.DwcaIO.Read;

import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwcaToAvroPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroPipeline.class);

  private DwcaToAvroPipeline() {}

  /** TODO: DOC! */
  public static void main(String[] args) {
    BasePipelineOptions options = PipelinesOptionsFactory.create(BasePipelineOptions.class, args);
    DwcaToAvroPipeline.createAndRun(options);
  }

  public static void createAndRun(BasePipelineOptions options) {
    LOG.info("Running the pipeline");
    create(options).run().waitUntilFinish();
    LOG.info("Pipeline has been finished");
  }

  /** TODO: DOC! */
  public static Pipeline create(BasePipelineOptions options) {

    String inputPath = options.getInputPath();
    String targetPath = FsUtils.buildPath(options, "verbatim");
    String tmpPath = FsUtils.getTempDir(options);

    boolean isDirectory = Paths.get(inputPath).toFile().isDirectory();
    Read reader = isDirectory ? Read.withPaths(inputPath) : Read.withPaths(inputPath, tmpPath);

    Pipeline p = Pipeline.create(options);

    p.apply("Read from Darwin Core Archive", reader)
        .apply("Write to avro", WriteTransforms.extended(targetPath).withoutSharding());

    return p;
  }
}
