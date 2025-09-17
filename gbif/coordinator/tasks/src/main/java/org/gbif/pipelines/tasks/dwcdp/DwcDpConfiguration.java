package org.gbif.pipelines.tasks.dwcdp;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to trigger the Airflow DAG to process DwcDP. */
@ToString
public class DwcDpConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  @ParametersDelegate @Valid @NotNull
  public IndexConfiguration indexConfig = new IndexConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AirflowConfiguration airflowConfig = new AirflowConfiguration();

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--archive-unpacked-repository")
  @NotNull
  public String archiveUnpackedRepository;

  public String getHdfsSiteConfig() {
    return stepConfig.hdfsSiteConfig;
  }

  public String getCoreSiteConfig() {
    return stepConfig.coreSiteConfig;
  }

  public String getRepositoryPath() {
    return stepConfig.repositoryPath;
  }
}
