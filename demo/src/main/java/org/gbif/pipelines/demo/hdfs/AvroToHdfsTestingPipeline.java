package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.HdfsExporterOptions;
import org.gbif.pipelines.demo.utils.PipelineUtils;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.util.Optional;

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

  private HdfsExporterOptions options;

  public AvroToHdfsTestingPipeline(HdfsExporterOptions options) {
    this.options = options;
  }

  public void createAndRunPipeline() {
    Optional.ofNullable(options).orElseThrow(() -> new IllegalArgumentException("Pipeline options cannot be null"));

    String targetPath = PipelineUtils.targetPath(options);

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
