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

/**
 * Callback mode for regular pipeline ingestion.
 *
 * <p>This mode uses the pipeline execution guard, creates tracking records for pipeline steps, and
 * queues downstream steps according to the regular occurrence/event workflow.
 */
@Slf4j
public class PipelineMode implements CallbackMode {

  /**
   * Skips processing when the current pipeline execution should not continue.
   *
   * <p>This protects against starting a new execution while another one is running and against
   * continuing an execution that has been stopped or superseded.
   */
  @Override
  public boolean shouldSkip(PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    return context.getExecutionGuard().isProcessingStopped(context.getMessage());
  }

  /** Creates or updates tracking records for the current pipeline step. */
  @Override
  public Optional<TrackingInfo> trackPipelineStep(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    return context.getTracker().track(context.getMessage(), context.getStepType());
  }

  /** Marks eligible downstream pipeline steps as queued using the regular pipeline workflow. */
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
                    i,
                    context.getStepType(),
                    context.getWorkflowResolver().pipelineWorkflow(context.getMessage())));
  }

  /** Regular ingestion does not require additional success handling. */
  @Override
  public void onSuccess(PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    // NOP
  }

  /** Regular ingestion failure is already reflected through pipeline step tracking. */
  @Override
  public void onFailure(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context, String errorMessage) {
    // NOP
  }

  @Override
  public void markPipelineExecutionIfFinished(
      Long executionId,
      PipelinesCallbackContext<? extends PipelineBasedMessage> context) {
    if (executionId != null) {
      log.info("Mark execution as FINISHED if all steps are FINISHED");
      context
          .getRetryingHistoryClient()
          .markPipelineExecutionIfFinished(executionId);
    }
  }
}
