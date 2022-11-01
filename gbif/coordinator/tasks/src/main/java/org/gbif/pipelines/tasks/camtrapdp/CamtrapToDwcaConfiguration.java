package org.gbif.pipelines.tasks.camtrapdp;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to convert downloaded CamtrapDP to DwC-A */
@ToString
public class CamtrapToDwcaConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.CAMTRAPDP_TO_DWCA + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--camtraptor-ws-url")
  @NotNull
  public String camtraptorWsUrl;

  @Parameter(names = "--gbif-api-ws-url")
  @NotNull
  public String gbifApiWsUrl;

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
