package org.gbif.pipelines.crawler.fragmenter;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to start raw fragments processing */
@ToString
public class FragmenterConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--number-threads")
  @Valid
  @NotNull
  @Min(1)
  public Integer numberThreads;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.FRAGMENTER + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--hbase-fragments-table")
  @Valid
  @NotNull
  public String hbaseFragmentsTable;

  @Parameter(names = "--dwca-archive-repository")
  @NotNull
  public String dwcaArchiveRepository;

  @Parameter(names = "--xml-archive-repository")
  @NotNull
  public String xmlArchiveRepository;

  @Parameter(names = "--xml-archive-repository-subdir")
  @NotNull
  public Set<String> xmlArchiveRepositorySubdir;

  @Parameter(names = "--async-threshold")
  public int asyncThreshold = 5_000;

  @Parameter(names = "--batch-size")
  public int batchSize = 100;

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
