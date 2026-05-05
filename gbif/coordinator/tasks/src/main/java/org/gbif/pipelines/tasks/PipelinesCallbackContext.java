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
package org.gbif.pipelines.tasks;

import lombok.Builder;
import lombok.Getter;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.tasks.client.RetryingDatasetClient;
import org.gbif.pipelines.tasks.client.RetryingPipelinesHistoryClient;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.pipelines.tasks.modes.CallbackMode;
import org.gbif.pipelines.tasks.tracking.PipelinesStepTracker;
import org.gbif.pipelines.tasks.tracking.QueuedStepUpdater;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * Internal context object shared between {@link PipelinesCallback}, {@link CallbackMode}, and
 * helper services.
 *
 * <p>The context groups all state and collaborators needed while handling a single callback
 * message. Passing this object to mode implementations keeps them independent from {@link
 * PipelinesCallback} internals and avoids exposing package-private methods from the callback
 * itself.
 *
 * <p>A context instance is created per callback invocation. It is intentionally package-private
 * because it is part of the callback implementation, not the public task API.
 *
 * @param <In> input message type handled by the callback
 */
@Getter
@Builder
public class PipelinesCallbackContext<In extends PipelineBasedMessage> {

  /** Message currently being handled. */
  private final In message;

  /** Pipeline step represented by the current callback. */
  private final StepType stepType;

  /** Runtime configuration used by helper services. */
  private final BaseConfiguration config;

  /** Publisher used to send messages to downstream queues. */
  private final MessagePublisher publisher;

  /** Client for pipeline history, execution, process, and step tracking. */
  private final PipelinesHistoryClient historyClient;

  /** Retry-aware wrapper around history-service calls used by callback helpers. */
  private final RetryingPipelinesHistoryClient retryingHistoryClient;

  /** Registry dataset client used for dataset-deletion checks. */
  private final DatasetClient datasetClient;

  /** Retry-aware wrapper around registry dataset calls. */
  private final RetryingDatasetClient retryingDatasetClient;

  /** Validator client used by validator-mode callbacks. */
  private final ValidationWsClient validationClient;

  /** Retry-aware wrapper around validator calls. */
  private final RetryingValidationClient retryingValidationClient;

  /** Service responsible for creating and updating pipeline tracking records. */
  private final PipelinesStepTracker tracker;

  /** Guard that decides whether regular pipeline execution should continue. */
  private final PipelinesExecutionGuard executionGuard;

  /** Service responsible for validator status checks and updates. */
  private final ValidatorStatusService validatorStatusService;

  /** Service responsible for marking downstream steps as queued. */
  private final QueuedStepUpdater queuedStepUpdater;

  /** Resolver for regular pipeline and validator workflows. */
  private final WorkflowResolver workflowResolver;
}
