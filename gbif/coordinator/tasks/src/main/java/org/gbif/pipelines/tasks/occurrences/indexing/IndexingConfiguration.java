package org.gbif.pipelines.tasks.occurrences.indexing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Indexing Pipeline on provided dataset */
@ToString
public class IndexingConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  @ParametersDelegate @Valid @NotNull
  public IndexConfiguration indexConfig = new IndexConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @ParametersDelegate @Valid @NotNull
  public StackableConfiguration stackableConfiguration = new StackableConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.OCCURRENCE_TO_INDEX + ".yml";

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--use-beam-deprecated-read")
  public boolean useBeamDeprecatedRead = true;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;

  @Parameter(names = "--es-generated-ids")
  public boolean esGeneratedIds = false;

  @Parameter(names = "--validator-listen-all-mq")
  public boolean validatorListenAllMq = true;

  @Parameter(names = "--back-pressure")
  public Integer backPressure;

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
}
