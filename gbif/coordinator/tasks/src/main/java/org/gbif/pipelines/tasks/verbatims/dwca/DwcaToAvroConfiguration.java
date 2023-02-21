package org.gbif.pipelines.tasks.verbatims.dwca;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import java.util.Collections;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** Configuration required to convert downloaded DwCArchive and etc to avro (ExtendedRecord) */
@ToString
public class DwcaToAvroConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.ARCHIVE_TO_VERBATIM + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--interpret-types")
  @NotNull
  public Set<String> interpretTypes = Collections.singleton(RecordType.ALL.name());

  @Parameter(names = "--file-name")
  @NotNull
  public String fileName = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;

  @Parameter(names = "--validator-only")
  public boolean validatorOnly = false;

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
