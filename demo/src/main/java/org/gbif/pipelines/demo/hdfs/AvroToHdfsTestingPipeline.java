package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.io.avro.UntypedOccurrence;
import org.gbif.pipelines.demo.utils.PipelineUtils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that writes an Avro file to HDFS.
 *
 * Created to run junit tests against it.
 */
public class AvroToHdfsTestingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToHdfsTestingPipeline.class);

  private Pipeline p;
  private HadoopFileSystemOptions hadoopOptions;
  private String targetPath;
  private String sourcePath;

  public AvroToHdfsTestingPipeline(HadoopFileSystemOptions hadoopOptions, String targetPath, String sourcePath) {
    this.hadoopOptions = hadoopOptions;
    this.targetPath = targetPath;
    this.sourcePath = sourcePath;
  }

  public void createAndRunPipeline() {
    if (hadoopOptions == null) {
      throw new IllegalArgumentException("Hadoop options cannot be null");
    }
    if (targetPath == null) {
      throw new IllegalArgumentException("targetPath cannot be null");
    }
    if (sourcePath == null) {
      throw new IllegalArgumentException("sourcePath cannot be null");
    }

    LOG.info("Target path : {}", targetPath);

    p = Pipeline.create(hadoopOptions);

    // Read Avro files
    PCollection<UntypedOccurrence> verbatimRecords =
      p.apply("Read Avro files", AvroIO.read(UntypedOccurrence.class).from(sourcePath));

    verbatimRecords.apply("Write Avro files",
                          AvroIO.write(UntypedOccurrence.class)
                            .to(targetPath)
                            .withTempDirectory(FileSystems.matchNewResource(PipelineUtils.tmpPath(hadoopOptions.getHdfsConfiguration()
                                                                                                    .get(0)), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  public void setHadoopOptions(HadoopFileSystemOptions hadoopOptions) {
    this.hadoopOptions = hadoopOptions;
  }

  public void setTargetPath(String targetPath) {
    this.targetPath = targetPath;
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }
}
