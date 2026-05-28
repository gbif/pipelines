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
package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Optional;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;

public interface DwcaArchiveValidatorOutgoingMessageCreator {

  PipelineBasedMessage createOutgoingMessage(
      ArchiveValidatorConfiguration config, PipelineBasedMessage message);

  static DatasetType getDatasetType(Archive archive) {
    Term rowType = archive.getCore().getRowType();
    if (rowType == DwcTerm.Occurrence) {
      return DatasetType.OCCURRENCE;
    }
    if (rowType == DwcTerm.Event) {
      return DatasetType.SAMPLING_EVENT;
    }
    if (rowType == DwcTerm.Taxon) {
      return DatasetType.CHECKLIST;
    }
    throw new IllegalArgumentException("DatasetType is not valid");
  }

  static Optional<DatasetType> getDatasetType(
      ArchiveValidatorConfiguration config, PipelineBasedMessage message) {
    try {
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      return Optional.of(getDatasetType(DwcaUtils.fromLocation(inputPath)));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
