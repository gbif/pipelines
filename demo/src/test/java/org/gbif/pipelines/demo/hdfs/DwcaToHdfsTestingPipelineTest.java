package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.core.config.HdfsExporterOptions;
import org.gbif.pipelines.demo.utils.PipelineUtils;

import java.io.File;
import java.net.URI;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the class {@link DwcaToHdfsTestingPipelineTest}.
 */
public class DwcaToHdfsTestingPipelineTest {

  private static final String DWCA_FILE_PATH = "data/dwca.zip";

  private static MiniDFSCluster hdfsCluster;
  private static Configuration configuration = new Configuration();
  private static FileSystem fs;
  private static URI hdfsClusterBaseUri;

  @BeforeClass
  public static void setUp() throws Exception {
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    fs = FileSystem.newInstance(configuration);
    hdfsClusterBaseUri = new URI(configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsClusterWhenWritingDwcaToHdfsThenFileCreated() throws Exception {

    // create options
    HdfsExporterOptions options = PipelineUtils.createPipelineOptions(configuration);
    options.setRunner(DirectRunner.class);

    options.setInputFile(DWCA_FILE_PATH);
    options.setDatasetId("123");
    options.setTargetDirectory(hdfsClusterBaseUri + "/pipelines");

    // create and run pipeline
    DwcaToHdfsTestingPipeline pipeline = new DwcaToHdfsTestingPipeline(options);
    pipeline.createAndRunPipeline();

    // test results
    URI uriTargetPath = hdfsClusterBaseUri.resolve(PipelineUtils.targetPath(options) + "*");
    FileStatus[] fileStatuses = fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(fs.exists(fileStatus.getPath()));
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPipelineOptionsTest() {

    // create options
    HdfsExporterOptions options = PipelineUtils.createPipelineOptions(configuration);
    options.setRunner(DirectRunner.class);

    // create and run pipeline
    DwcaToHdfsTestingPipeline pipeline = new DwcaToHdfsTestingPipeline(options);
    pipeline.createAndRunPipeline();

  }

}
