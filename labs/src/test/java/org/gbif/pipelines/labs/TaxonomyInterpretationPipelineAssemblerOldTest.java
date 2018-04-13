package org.gbif.pipelines.labs;

import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.OptionsKeyEnum;
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

import static org.gbif.pipelines.config.DataPipelineOptionsFactory.createDefaultTaxonOptions;

/**
 * Tests the {@link TaxonomyInterpretationPipeline}.
 */
public class TaxonomyInterpretationPipelineAssemblerOldTest {

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

    DataProcessingPipelineOptions options = createDefaultTaxonOptions(configuration,
                                                                                    sourcePath,
                                                                                    taxonOutPath,
                                                                                    issuesOutPath,
                                                                                    new String[] {
                                                                                      "--runner=DirectRunner"});

    TaxonomyInterpretationPipeline.runPipelineProgrammatically(options);

    // test taxon results
    URI uriTargetPath = clusterConfig.hdfsClusterBaseUri.resolve(options.getTargetPaths()
                                                                   .get(OptionsKeyEnum.GBIF_BACKBONE)
                                                                   .filePath() + "*");
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
      clusterConfig.hdfsClusterBaseUri.resolve(options.getTargetPaths().get(OptionsKeyEnum.ISSUES).filePath()
                                               + "*");
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
