package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.TargetPath;
import org.gbif.pipelines.demo.TestUtils;
import org.gbif.pipelines.demo.utils.PipelineUtils;

import java.net.URI;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the class {@link AvroToHdfsTestingPipeline}.
 */
public class AvroToHdfsTestingPipelineTest {

  private static final String AVRO_FILE_PATH = "data/exportData*";

  private static TestUtils.MiniClusterConfig clusterConfig;
  private static Configuration configuration = new Configuration();

  @BeforeClass
  public static void setUp() throws Exception {
    clusterConfig = TestUtils.createMiniCluster(configuration);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clusterConfig.hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsClusterWhenWritingAvroToHdfsThenFileCreated() throws Exception {

    // create options
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptions(clusterConfig.configuration);
    options.setRunner(DirectRunner.class);

    options.setInputFile(AVRO_FILE_PATH);
    options.setDatasetId("123");
    options.setDefaultTargetDirectory(clusterConfig.hdfsClusterBaseUri + "pipelines");

    // create and run pipeline
    AvroToHdfsTestingPipeline pipeline = new AvroToHdfsTestingPipeline(options);
    pipeline.createAndRunPipeline();

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

}
