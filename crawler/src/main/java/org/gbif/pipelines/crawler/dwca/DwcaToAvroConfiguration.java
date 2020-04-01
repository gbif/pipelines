package org.gbif.pipelines.crawler.dwca;

import java.util.Collections;
import java.util.Set;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.RegistryConfiguration;
import org.gbif.pipelines.common.configs.ZooKeeperConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to convert downloaded DwCArchive and etc to avro (ExtendedRecord)
 */
@ToString
public class DwcaToAvroConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.ARCHIVE_TO_VERBATIM + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--repository-path")
  @NotNull
  public String repositoryPath;

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--hdfs-site-config")
  public String hdfsSiteConfig;

  @Parameter(names = "--interpret-types")
  @NotNull
  public Set<String> interpretTypes = Collections.singleton(RecordType.ALL.name());

  @Parameter(names = "--file-name")
  @NotNull
  public String fileName = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

}
