package org.gbif.pipelines.tasks.occurrences.interpretation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Interpretation Pipeline on provided dataset */
@ToString
public class InterpreterConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AirflowConfiguration airflowConfig = new AirflowConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VERBATIM_TO_OCCURRENCE + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;

  @Parameter(names = "--skip-gbif-ids")
  public boolean skipGbifIds = false;

  @Parameter(names = "--validator-listen-all-mq")
  public boolean validatorListenAllMq = true;

  @Parameter(names = "--delete-after-days")
  public long deleteAfterDays = 7L;

  @Parameter(names = "--fail-if-duplicate-id-percent")
  public int failIfDuplicateIdPercent = 5;

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
