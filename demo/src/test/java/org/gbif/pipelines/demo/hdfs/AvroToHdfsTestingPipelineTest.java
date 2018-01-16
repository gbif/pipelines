package org.gbif.pipelines.demo.hdfs;

import java.io.File;
import java.net.URI;
import java.util.Collections;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the class {@link AvroToHdfsTestingPipelineTest}.
 */
public class AvroToHdfsTestingPipelineTest {

  private MiniDFSCluster hdfsCluster;
  private Configuration configuration = new Configuration();
  private FileSystem fs;
  private URI hdfsClusterBaseUri;

  @Before
  public void setUp() throws Exception {
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    fs = FileSystem.newInstance(configuration);
    hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");
  }

  @After
  public void tearDown() throws Exception {
    hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsCluster_WhenWritingToHdfs_ThenFileCreated() throws Exception {

    HadoopFileSystemOptions options = PipelineOptionsFactory.as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(configuration));
    options.setRunner(DirectRunner.class);

    String targetPath = configuration.get("fs.defaultFS") + "/pipelines1";
    String sourcePath = "data/exportData*";

    AvroToHdfsTestingPipeline pipeline = new AvroToHdfsTestingPipeline(options, targetPath, sourcePath);
    pipeline.createAndRunPipeline();

    URI uriTargetPath = hdfsClusterBaseUri.resolve(targetPath + "*");
    FileStatus[] fileStatuses = fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(fs.exists(fileStatus.getPath()));
    }

  }

}
