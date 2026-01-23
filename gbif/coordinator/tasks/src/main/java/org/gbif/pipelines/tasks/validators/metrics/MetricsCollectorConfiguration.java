package org.gbif.pipelines.tasks.validators.metrics;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.ElasticsearchConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to collect metrics using ES index */
@ToString
public class MetricsCollectorConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.COLLECT_METRICS + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--index-name")
  public String indexName = "validator";

  @Parameter(names = "--core-prefix")
  public String corePrefix = "verbatim.core";

  @Parameter(names = "--extensions-prefix")
  public String extensionsPrefix = "verbatim.extensions";

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;

  @Parameter(names = "--validator-checklist-timeout-min")
  public long checklistTimeoutMin = 60;

  @Parameter(names = "--interpretation-meta-file-name")
  public String interpretationMetaFileName = Pipeline.VERBATIM_TO_OCCURRENCE + ".yml";

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
