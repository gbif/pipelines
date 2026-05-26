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
package org.gbif.pipelines.tasks.modes;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.tasks.PipelinesCallbackContext;
import org.gbif.pipelines.tasks.tracking.TrackingInfo;
import org.gbif.validator.api.Validation;

/**
 * Callback mode for validator processing.
 *
 * <p>Validator callbacks do not create regular pipeline tracking records. Instead, they use the
 * validation service to decide whether processing should continue and to update validation status
 * after success or failure.
 */
@Slf4j
public class ValidatorMode implements CallbackMode {

  /** Skips processing when the validation record is already in a state that should not continue. */
  @Override
  public boolean shouldSkip(PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    return context.getValidatorStatusService().isValidatorAborted(context.getMessage());
  }

  /**
   * Validator callbacks do not create regular pipeline step tracking records.
   *
   * <p>The validation status is tracked through the validator service instead.
   */
  @Override
  public Optional<TrackingInfo> trackPipelineStep(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    log.info("Skipping pipeline tracking for validator callback");
    return Optional.empty();
  }

  /** Marks eligible downstream validator steps as queued using the validator workflow. */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @Override
  public void updateQueuedStatus(
      Optional<TrackingInfo> info,
      PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    info.ifPresent(
        i ->
            context
                .getQueuedStepUpdater()
                .updateQueuedStatus(
                    i, context.getStepType(), context.getWorkflowResolver().validatorWorkflow()));
  }

  /** Marks validation as finished after successful validator processing. */
  @Override
  public void onSuccess(PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    context
        .getValidatorStatusService()
        .updateStatus(context.getMessage(), context.getStepType(), Validation.Status.FINISHED);
  }

  /** Marks validation as failed after validator processing throws an exception. */
  @Override
  public void onFailure(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context, String errorMessage) {
    context
        .getValidatorStatusService()
        .updateStatus(
            context.getMessage(), context.getStepType(), Validation.Status.FAILED, errorMessage);
  }
}
