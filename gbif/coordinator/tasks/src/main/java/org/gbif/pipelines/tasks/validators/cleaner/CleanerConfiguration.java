package org.gbif.pipelines.tasks.validators.cleaner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to start Hdfs View processing */
@ToString
public class CleanerConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--es-hosts")
  public String[] esHosts;

  @Parameter(names = "--es-aliases")
  public String[] esAliases;

  @Parameter(names = "--es-search-query-timeout-sec")
  public int esSearchQueryTimeoutSec = 5;

  @Parameter(names = "--es-search-query-attempts")
  public int esSearchQueryAttempts = 200;

  @Parameter(names = "--fs-root-path")
  public String fsRootPath;

  @Parameter(names = "--hdfs-root-path")
  public String hdfsRootPath;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;
}
