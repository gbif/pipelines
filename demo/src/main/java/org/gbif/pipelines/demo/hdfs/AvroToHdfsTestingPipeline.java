package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.TargetPath;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that writes an Avro file to HDFS.
 * <p>
 * Created to run junit tests against it.
 */
public class AvroToHdfsTestingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToHdfsTestingPipeline.class);

  private DataProcessingPipelineOptions options;

  public AvroToHdfsTestingPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public void createAndRunPipeline() {
    Objects.requireNonNull(options, "Pipeline options cannot be null");

    String targetPath = TargetPath.fullPath(options.getDefaultTargetDirectory(), options.getDatasetId());

    LOG.info("Target path : {}", targetPath);

    Pipeline pipeline = Pipeline.create(options);

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
