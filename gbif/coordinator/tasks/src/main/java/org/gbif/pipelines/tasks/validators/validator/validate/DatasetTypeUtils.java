package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.DwcaUtils;

@Slf4j
public class DatasetTypeUtils {

  /** Gets the dataset type from the Archive parameter. */
  public static DatasetType getDatasetType(Archive archive) {
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

  /** Gets the dataset type form the current archive data. */
  public static Optional<DatasetType> getDatasetType(String archiveRepository, UUID datasetUuid) {
    try {
      Path inputPath = buildDwcaInputPath(archiveRepository, datasetUuid);
      return Optional.of(getDatasetType(DwcaUtils.fromLocation(inputPath)));
    } catch (Exception ex) {
      log.error("Couldn't get dataset type for validation {}", datasetUuid, ex);
      return Optional.empty();
    }
  }
}
