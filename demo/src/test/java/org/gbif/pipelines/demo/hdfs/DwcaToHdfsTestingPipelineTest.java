package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.config.TargetPath;
import org.gbif.pipelines.demo.TestUtils;
import org.gbif.pipelines.demo.utils.PipelineUtils;

import java.net.URI;
import java.util.Map;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the class {@link DwcaToHdfsTestingPipeline}.
 */
public class DwcaToHdfsTestingPipelineTest {

  private static final String DWCA_FILE_PATH = "data/dwca.zip";

  private static TestUtils.MiniClusterConfig clusterConfig;
  private static Configuration configuration = new Configuration();

  @BeforeClass
  public static void setUp() throws Exception {
    clusterConfig = TestUtils.createMiniCluster(configuration);
  }

  @AfterClass
  public static void tearDown() {
    clusterConfig.hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsClusterWhenWritingDwcaToHdfsThenFileCreated() throws Exception {

    // create options
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptions(clusterConfig.configuration);
    options.setRunner(DirectRunner.class);

    options.setInputFile(DWCA_FILE_PATH);
    options.setDatasetId("123");
    options.setDefaultTargetDirectory(clusterConfig.hdfsClusterBaseUri + "/pipelines");

    // create and run pipeline
    DwcaToHdfsTestingPipeline pipeline = new DwcaToHdfsTestingPipeline(options);
    pipeline.createAndRunPipeline();

    // test results
    URI uriTargetPath =
      clusterConfig.hdfsClusterBaseUri.resolve(TargetPath.fullPath(options.getDefaultTargetDirectory(),
                                                                   options.getDatasetId()) + "*");
    FileStatus[] fileStatuses = clusterConfig.fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(clusterConfig.fs.exists(fileStatus.getPath()));
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPipelineOptionsTest() {

    // create options
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptions(clusterConfig.configuration);
    options.setRunner(DirectRunner.class);

    // create and run pipeline
    DwcaToHdfsTestingPipeline pipeline = new DwcaToHdfsTestingPipeline(options);
    pipeline.createAndRunPipeline();

  }

  @Test
  public void defaultTargetPathsTest() {
    // create options
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptions(clusterConfig.configuration);

    Map<RecordInterpretation, TargetPath> targetPaths = options.getTargetPaths();

    Assert.assertNotNull(targetPaths);
    Assert.assertEquals(RecordInterpretation.values().length, targetPaths.size());

    for (RecordInterpretation recordInterpretation : RecordInterpretation.values()) {
      TargetPath tp = targetPaths.get(recordInterpretation);

      Assert.assertNotNull(tp);
      Assert.assertEquals(tp.getDirectory(), options.getDefaultTargetDirectory());
      Assert.assertEquals(tp.getFileName(), recordInterpretation.getDefaultFileName());
    }

  }

}
