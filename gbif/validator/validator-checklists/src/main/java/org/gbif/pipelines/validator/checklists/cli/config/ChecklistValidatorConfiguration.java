package org.gbif.pipelines.validator.checklists.cli.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.io.File;
import lombok.ToString;
import org.gbif.cli.PropertyName;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;

/** Configuration required to validate downloaded archive */
@ToString
public class ChecklistValidatorConfiguration {

  @ParametersDelegate @Valid @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VALIDATOR + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--neo-repository")
  public File neoRepository = new File("/tmp/neo");

  @Parameter(names = "--gbif-api-url")
  @PropertyName("gbif.api.url")
  public String gbifApiUrl;

  @Parameter(names = "--neo-batch-size")
  public int neoBatchSize = 10000;

  @Parameter(names = "--neo-mapped-memory")
  public int neoMappedMemory = 128;

  @Parameter(names = "--neo-port")
  public int neoPort = 1337;
}
