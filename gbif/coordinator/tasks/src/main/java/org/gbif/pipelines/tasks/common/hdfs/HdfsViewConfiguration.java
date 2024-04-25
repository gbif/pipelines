package org.gbif.pipelines.tasks.common.hdfs;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.api.model.pipelines.InterpretationType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Hdfs View processing */
@ToString
public class HdfsViewConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @ParametersDelegate @Valid @NotNull
  public StackableConfiguration stackableConfiguration = new StackableConfiguration();

  @Parameter(names = "--repository-target-path")
  @NotNull
  public String repositoryTargetPath;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.OCCURRENCE_TO_HDFS + ".yml";

  @Parameter(names = "--hdfs-avro-coefficient-ratio")
  public int hdfsAvroCoefficientRatio = 75;

  @Parameter(names = "--hdfs-avro-expected-file-size-in-mb")
  public int hdfsAvroExpectedFileSizeInMb = 2_048; // 2GB

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--step-type")
  public StepType stepType = StepType.HDFS_VIEW;

  @Parameter(names = "--record-type")
  public InterpretationType.RecordType recordType = InterpretationType.RecordType.OCCURRENCE;

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
