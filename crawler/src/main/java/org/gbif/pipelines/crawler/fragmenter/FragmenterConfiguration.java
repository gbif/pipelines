package org.gbif.pipelines.crawler.fragmenter;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.RegistryConfiguration;
import org.gbif.pipelines.common.configs.ZooKeeperConfiguration;
import org.gbif.pipelines.crawler.BaseConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to start raw fragments processing
 */
@ToString
public class FragmenterConfiguration implements BaseConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

  @Parameter(names = "--repository-path")
  @NotNull
  public String repositoryPath;

  @Parameter(names = "--number-threads")
  public Integer numberThreads;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.FRAGMENTER + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Override
  public String getHdfsSiteConfig() {
    return hdfsSiteConfig;
  }

  @Override
  public String getRepositoryPath() {
    return repositoryPath;
  }

  @Override
  public String getMetaFileName() {
    return metaFileName;
  }

}
