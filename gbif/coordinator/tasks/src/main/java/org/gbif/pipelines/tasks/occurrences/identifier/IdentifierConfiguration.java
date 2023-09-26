package org.gbif.pipelines.tasks.occurrences.identifier;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.util.Collections;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Interpretation Pipeline on provided dataset */
@ToString
public class IdentifierConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @ParametersDelegate @Valid @NotNull
  public StackableConfiguration stackableConfiguration = new StackableConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VERBATIM_TO_IDENTIFIER + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--use-beam-deprecated-read")
  public boolean useBeamDeprecatedRead = true;

  @Parameter(names = "--id-threshold-percent")
  public double idThresholdPercent = 5;

  @Parameter(names = "--id-threshold-skip")
  public boolean idThresholdSkip = false;

  @Parameter(names = "--ignore-checklists")
  public boolean ignoreChecklists = true;

  @Parameter(names = "--skip-installations-list")
  public Set<String> skipInstallationsList = Collections.emptySet();

  // Since we have GitHub issue creation for failed dataset we can skip manual stage finishing
  @Parameter(names = "--clean-and-mark-as-aborted")
  public boolean cleanAndMarkAsAborted = false;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

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
