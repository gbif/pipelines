package org.gbif.pipelines.labs;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.TargetPath;
import org.gbif.pipelines.io.avro.UntypedOccurrence;
import org.gbif.pipelines.labs.util.HdfsTestUtils;

import java.net.URI;
import java.util.Objects;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the class {@link AvroToHdfsPipelineTest}.
 */
@Ignore("Must not be a part of main build")
public class AvroToHdfsPipelineTest {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToHdfsPipelineTest.class);

  private static final String AVRO_FILE_PATH = "data/exportData*";

  private static HdfsTestUtils.MiniClusterConfig clusterConfig;
  private static Configuration configuration = new Configuration();

  @BeforeClass
  public static void setUp() throws Exception {
    clusterConfig = HdfsTestUtils.createMiniCluster(configuration);
  }

  @AfterClass
  public static void tearDown() {
    clusterConfig.hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsClusterWhenWritingAvroToHdfsThenFileCreated() throws Exception {

    // create options
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);
    options.setRunner(DirectRunner.class);

    options.setInputFile(AVRO_FILE_PATH);
    options.setDatasetId("123");
    options.setDefaultTargetDirectory(clusterConfig.hdfsClusterBaseUri + "pipelines");

    // create and run pipeline
    createAndRunPipeline(options);

    // test results
    URI uriTargetPath =
      clusterConfig.hdfsClusterBaseUri.resolve(TargetPath.fullPath(options.getDefaultTargetDirectory(), options.getDatasetId())
                                 + "*");
    FileStatus[] fileStatuses = clusterConfig.fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(clusterConfig.fs.exists(fileStatus.getPath()));
    }

  }

  private void createAndRunPipeline(DataProcessingPipelineOptions options) {
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
