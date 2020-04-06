package org.gbif.pipelines.crawler.balancer;

import org.gbif.pipelines.common.configs.StepConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to start Balancer service
 */
@ToString
public class BalancerConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public StepConfiguration stepConfig = new StepConfiguration();

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
