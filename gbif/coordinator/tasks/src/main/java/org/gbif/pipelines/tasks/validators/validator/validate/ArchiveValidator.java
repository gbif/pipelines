package org.gbif.pipelines.tasks.validators.validator.validate;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.validator.api.Validation;

public interface ArchiveValidator {

  PipelineBasedMessage createOutgoingMessage();

  default void validate() {
    // NOP
  }

  default Validation.Status getFinalValidationStatus() {
    return Validation.Status.FINISHED;
  }

  /**
   * Orchestrator mode: validates without validator-ws (no GET or PUT); the result goes into the
   * step's completion. A checklist is submitted to ChecklistBank and the submission awaited.
   *
   * @throws UnsupportedOperationException for formats the orchestrated validation does not cover
   */
  default OrchestratedArchiveValidation validateForOrchestrator() throws Exception {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " has no orchestrated validation");
  }
}
