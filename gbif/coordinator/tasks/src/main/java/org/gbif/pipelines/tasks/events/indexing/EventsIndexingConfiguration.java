package org.gbif.pipelines.tasks.events.indexing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Indexing Pipeline on provided dataset */
@ToString
public class EventsIndexingConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @ParametersDelegate @Valid @NotNull
  public StackableConfiguration stackableConfiguration = new StackableConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.EVENT_TO_INDEX + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @ParametersDelegate @Valid @NotNull
  public IndexConfiguration indexConfig = new IndexConfiguration();

  @Parameter(names = "--es-generated-ids")
  public boolean esGeneratedIds = false;

  @Parameter(names = "--use-beam-deprecated-read")
  public boolean useBeamDeprecatedRead = true;

  @ParametersDelegate @Valid @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

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
