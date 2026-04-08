package org.gbif.pipelines.tasks.balancer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to start Balancer service */
@ToString
public class BalancerConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--switch-file-size-mb")
  @NotNull
  @Min(1)
  public int switchFileSizeMb;

  @Parameter(names = "--validator-switch-records-number")
  @NotNull
  @Min(1)
  public int validatorSwitchRecordsNumber;

  @Parameter(names = "--validator-repository-path")
  public String validatorRepositoryPath;
}
