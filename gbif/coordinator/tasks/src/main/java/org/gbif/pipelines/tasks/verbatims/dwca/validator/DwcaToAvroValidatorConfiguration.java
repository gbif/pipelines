/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.tasks.verbatims.dwca.validator;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Set;
import lombok.ToString;
import org.gbif.api.model.pipelines.InterpretationType;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

/**
 * Configuration required to convert downloaded DwC archives to avro (ExtendedRecord). Similar to
 * {@link org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration} but for Pipelines
 * Validator.
 */
@ToString
public class DwcaToAvroValidatorConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = PipelinesVariables.Pipeline.ARCHIVE_TO_VERBATIM + ".yml";

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--interpret-types")
  @NotNull
  public Set<String> interpretTypes =
      Collections.singleton(InterpretationType.RecordType.ALL.name());

  @Parameter(names = "--file-name")
  @NotNull
  public String fileName =
      PipelinesVariables.Pipeline.Conversion.FILE_NAME + PipelinesVariables.Pipeline.AVRO_EXTENSION;

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
