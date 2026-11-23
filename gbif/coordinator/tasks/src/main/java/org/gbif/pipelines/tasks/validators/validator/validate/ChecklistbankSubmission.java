package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.validator.checklist.ChecklistValidator;

/**
 * Orchestrator mode: submits a checklist upload to ChecklistBank and waits for the dataset key, so
 * the archive step can report it (the legacy validators stored it in validator-ws from an
 * asynchronous callback, racing their own status update). ChecklistBank imports it afterwards and
 * calls the validation web service back.
 */
@Slf4j
final class ChecklistbankSubmission {

  /** How long ChecklistBank may take to accept the upload (not to import it). */
  static final long SUBMIT_TIMEOUT_MINUTES = 30;

  private ChecklistbankSubmission() {}

  static int submit(ChecklistValidator checklistValidator, String archiveRepository, UUID key)
      throws Exception {
    Path archive = UploadedArchive.find(buildDwcaInputPath(archiveRepository, key));
    Integer datasetKey =
        checklistValidator
            .submitValidation(archive, key)
            .get(SUBMIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    if (datasetKey == null || datasetKey == 0) {
      throw new IllegalStateException("ChecklistBank returned no dataset key for " + archive);
    }
    log.info("Validation {} submitted to ChecklistBank as dataset {}", key, datasetKey);
    return datasetKey;
  }
}
