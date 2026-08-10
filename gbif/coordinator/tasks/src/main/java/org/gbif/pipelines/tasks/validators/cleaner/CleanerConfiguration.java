package org.gbif.pipelines.tasks.validators.cleaner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to start Hdfs View processing */
@ToString
public class CleanerConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--fs-root-path")
  public String fsRootPath;

  @Parameter(names = "--hdfs-root-path")
  public String hdfsRootPath;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;
}
