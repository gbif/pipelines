package org.gbif.pipelines.demo.hdfs;

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
 *
 * Source path is hard coded for demo purposes.
 */
public class AvroToHdfsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToHdfsPipeline.class);

  /**
   * Main method.
   *
   * @param args beam arguments + -targetPath to specify the path where the files should be written.
   */
  public static void main(String[] args) {

    String targetPath = PipelineUtils.getTargetPath(args);

    LOG.info("Target path : {}", targetPath);

    Configuration config = new Configuration();
    Pipeline p = PipelineUtils.newHadoopPipeline(args, config);

    // Read Avro files
    PCollection<UntypedOccurrence> verbatimRecords =
      p.apply("Read Avro files", AvroIO.read(UntypedOccurrence.class).from("/home/mlopez/tests/exportData*"));

    verbatimRecords.apply("Write Avro files",
                          AvroIO.write(UntypedOccurrence.class)
                            .to(targetPath)
                            .withTempDirectory(FileSystems.matchNewResource(PipelineUtils.tmpPath(config), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());

  }

}
