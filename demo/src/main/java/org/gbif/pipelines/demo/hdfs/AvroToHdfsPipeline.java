package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.TargetPath;
import org.gbif.pipelines.demo.utils.PipelineUtils;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that writes an Avro file to HDFS.
 * <p>
 * Source path is hard coded for demo purposes.
 */
public class AvroToHdfsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToHdfsPipeline.class);

  /**
   * Main method.
   */
  public static void main(String[] args) {

    Configuration config = new Configuration();
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptions(config, args);
    Pipeline pipeline = Pipeline.create(options);

    String targetPath = TargetPath.fullPath(options.getDefaultTargetDirectory(), options.getDatasetId());

    LOG.info("Target path : {}", targetPath);

    // Read Avro files
    PCollection<UntypedOccurrence> verbatimRecords =
      pipeline.apply("Read Avro files", AvroIO.read(UntypedOccurrence.class).from(options.getInputFile()));

    verbatimRecords.apply("Write Avro files",
                          AvroIO.write(UntypedOccurrence.class)
                            .to(targetPath)
                            .withTempDirectory(FileSystems.matchNewResource(options.getHdfsTempLocation(), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());

  }

}
