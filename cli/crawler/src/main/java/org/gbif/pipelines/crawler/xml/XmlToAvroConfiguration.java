package org.gbif.pipelines.crawler.xml;

import java.util.Collections;
import java.util.Set;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to convert downloaded ABCD and etc to avro (ExtendedRecord)
 */
@ToString
public class XmlToAvroConfiguration implements BaseConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.ARCHIVE_TO_VERBATIM + ".yml";

  @Parameter(names = "--xml-reader-parallelism")
  @Min(1)
  public Integer xmlReaderParallelism;

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--archive-repository-subdir")
  @NotNull
  public Set<String> archiveRepositorySubdir;

  @Parameter(names = "--interpret-types")
  @NotNull
  public Set<String> interpretTypes = Collections.singleton(RecordType.ALL.name());

  @Parameter(names = "--file-name")
  @NotNull
  public String fileName = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;

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
