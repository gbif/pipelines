package org.gbif.pipelines.tasks.events;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.DistributedConfiguration;
import org.gbif.pipelines.common.configs.ElasticsearchConfiguration;
import org.gbif.pipelines.common.configs.IndexConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to start Indexing Pipeline on provided dataset */
@ToString
public class EventsConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  // TODO
  //  @Parameter(names = "--meta-file-name")
  //  public String metaFileName = Pipeline.VERBATIM_TO_INTERPRETED + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @ParametersDelegate @Valid @NotNull
  public IndexConfiguration indexConfig = new IndexConfiguration();

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
    // TODO
    return null;
  }
}
