package org.gbif.pipelines.crawler.balancer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.common.messaging.config.MessagingConfiguration;

/** Configuration required to start Balancer service */
@ToString
public class BalancerConfiguration {

  @ParametersDelegate @Valid @NotNull
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
}
