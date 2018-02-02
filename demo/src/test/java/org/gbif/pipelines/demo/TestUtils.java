package org.gbif.pipelines.demo;

import java.io.File;
import java.net.URI;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Utility class for testing purposes.
 */
public final class TestUtils {

  private TestUtils() {}

  /**
   * Creates a HDFS mini cluster.
   *
   * @param configuration configuration to be used
   *
   * @return Config of the cluster
   *
   * @throws Exception if the cluster could not be created
   */
  public static MiniClusterConfig createMiniCluster(Configuration configuration) throws Exception {
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);

    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniClusterConfig clusterConfig = new MiniClusterConfig();
    clusterConfig.configuration = configuration;
    clusterConfig.hdfsCluster = new MiniDFSCluster.Builder(configuration).build();
    clusterConfig.fs = FileSystem.newInstance(configuration);
    clusterConfig.hdfsClusterBaseUri =
      new URI(configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/");

    return clusterConfig;
  }

  @VisibleForTesting
  public static class MiniClusterConfig {

    // all attributes made public just to make it easier for testing
    public MiniDFSCluster hdfsCluster;
    public Configuration configuration = new Configuration();
    public FileSystem fs;
    public URI hdfsClusterBaseUri;
  }

}
