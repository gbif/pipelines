package org.gbif.pipelines.tasks.validators.validator;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to validate downloaded archive */
@ToString
public class ArchiveValidatorConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VALIDATOR + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--max-example-errors")
  public int maxExampleErrors = 100;

  @Parameter(names = "--max-records")
  public int maxRecords = 2_000_000;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;

  @Override
  public String getHdfsSiteConfig() {
    return stepConfig.hdfsSiteConfig;
  }

  @Override
  public String getCoreSiteConfig() {
    return stepConfig.coreSiteConfig;
  }

  @Override
  public String getRepositoryPath() {
    return stepConfig.repositoryPath;
  }

  @Override
  public String getMetaFileName() {
    return metaFileName;
  }

  @Override
  public boolean eventsEnabled() {
    return stepConfig.eventsEnabled;
  }
}
