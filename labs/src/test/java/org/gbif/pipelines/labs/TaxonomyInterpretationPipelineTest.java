package org.gbif.pipelines.labs;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.labs.util.HdfsTestUtils;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the {@link TaxonomyInterpretationPipeline}.
 */
@Ignore("Must not be a part of main build")
public class TaxonomyInterpretationPipelineTest {

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
  @Ignore("No longer needed")
  public void taxonomyInterpretationPipelineTest() throws Exception {

    final String sourcePath = "data/extendedRecords*";
    final String taxonOutPath = clusterConfig.hdfsClusterBaseUri + "taxon";
    final String issuesOutPath = clusterConfig.hdfsClusterBaseUri + "taxonIssues";

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);
    options.setInputFile(sourcePath);

    TaxonomyInterpretationPipeline.runPipeline(options);

    // test taxon results
    URI uriTargetPath = clusterConfig.hdfsClusterBaseUri.resolve(taxonOutPath + "*");
    FileStatus[] fileStatuses = clusterConfig.fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(clusterConfig.fs.exists(fileStatus.getPath()));
    }

    // test issues results
    uriTargetPath =
      clusterConfig.hdfsClusterBaseUri.resolve(issuesOutPath+ "*");
    fileStatuses = clusterConfig.fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(clusterConfig.fs.exists(fileStatus.getPath()));
    }

  }

}
