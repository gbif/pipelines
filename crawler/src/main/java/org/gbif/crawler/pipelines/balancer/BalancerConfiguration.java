package org.gbif.crawler.pipelines.balancer;

import org.gbif.common.messaging.config.MessagingConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.MoreObjects;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration required to start Balancer service
 */
public class BalancerConfiguration {

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

  @Parameter(names = "--switch-files-number")
  @NotNull
  @Min(1)
  public int switchFilesNumber;

  @Parameter(names = "--switch-file-size-mb")
  @NotNull
  @Min(1)
  public int switchFileSizeMb;

  @Parameter(names = "--switch-records-number")
  @NotNull
  @Min(1)
  public int switchRecordsNumber;

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("messaging", messaging)
        .add("queueName", queueName)
        .add("poolSize", poolSize)
        .add("hdfsSiteConfig", hdfsSiteConfig)
        .add("coreSiteConfig", coreSiteConfig)
        .add("repositoryPath", repositoryPath)
        .toString();
  }

}
