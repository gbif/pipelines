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
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.PipelinesCallbackContext;
import org.gbif.pipelines.tasks.tracking.TrackingInfo;

/**
 * Defines mode-specific behavior for {@link PipelinesCallback}.
 *
 * <p>The callback lifecycle is shared for regular pipeline ingestion and validator processing, but
 * some decisions differ between those modes:
 *
 * <ul>
 *   <li>whether the message should be skipped before execution
 *   <li>whether pipeline step tracking should be created
 *   <li>which workflow graph should be used when queueing downstream steps
 *   <li>what should happen after successful or failed processing
 * </ul>
 *
 * <p>This interface keeps those differences out of {@link PipelinesCallback}. Implementations
 * should contain only mode-specific decisions and delegate infrastructure work to services
 * available from {@link PipelinesCallbackContext}.
 */
public interface CallbackMode {

  /** Returns the regular ingestion mode. */
  static CallbackMode pipelines() {
    return new PipelineMode();
  }

  /** Returns the validator mode. */
  static CallbackMode validator() {
    return new ValidatorMode();
  }

  /**
   * Returns {@code true} when the current message should not be processed.
   *
   * <p>For regular ingestion this usually means another execution is running or the current
   * execution has been stopped. For validation this usually means the validation record is already
   * in a terminal or otherwise non-processable state.
   */
  boolean shouldSkip(PipelinesCallbackContext<? extends PipelineBasedMessage> context);

  /**
   * Performs mode-specific tracking before task execution.
   *
   * <p>Regular ingestion creates or updates pipeline process/execution/step tracking records.
   * Validator mode does not create pipeline tracking records and returns {@link Optional#empty()}.
   */
  Optional<TrackingInfo> trackPipelineStep(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context);

  /**
   * Marks downstream steps as queued after the outgoing message has been published.
   *
   * <p>The selected mode decides which workflow graph should be used: regular ingestion uses the
   * occurrence/event workflow, while validation uses the validator workflow.
   */
  void updateQueuedStatus(
      TrackingInfo info, PipelinesCallbackContext<? extends PipelineBasedMessage> context);

  /**
   * Performs mode-specific success handling after the message has been processed.
   *
   * <p>Regular ingestion does not need additional success handling. Validator mode updates the
   * validation status.
   */
  void onSuccess(PipelinesCallbackContext<? extends PipelineBasedMessage> context);

  /**
   * Performs mode-specific failure handling after an exception.
   *
   * @param errorMessage short error text to store when available; may be {@code null}
   */
  void onFailure(
      PipelinesCallbackContext<? extends PipelineBasedMessage> context, String errorMessage);
}
