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

import lombok.RequiredArgsConstructor;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.configs.BaseConfiguration;

/**
 * Resolves the {@link PipelinesWorkflow.Graph} that should be used for a pipeline message.
 *
 * <p>The pipeline workflow is selected from dataset capabilities carried by the message:
 *
 * <ul>
 *   <li>whether the dataset contains occurrence records
 *   <li>whether the dataset contains event records
 * </ul>
 *
 * <p>Event processing is enabled only when both conditions are true:
 *
 * <ul>
 *   <li>the application configuration enables event pipelines
 *   <li>the message describes a sampling-event dataset containing events
 * </ul>
 *
 * <p>Validator workflows are resolved separately because validation uses its own fixed workflow
 * graph and should not depend on the occurrence/event workflow selection logic.
 */
@RequiredArgsConstructor
public class WorkflowResolver {

  private final BaseConfiguration config;

  /**
   * Returns the regular ingestion workflow for the supplied message.
   *
   * <p>The workflow is chosen according to the message dataset information and the current
   * application configuration. This allows occurrence-only, event-only, and mixed-capability
   * datasets to follow the correct pipeline graph.
   */
  public Graph<StepType> pipelineWorkflow(PipelineBasedMessage message) {
    boolean containsEvents = containsEvents(message);
    boolean containsOccurrences = containsOccurrences(message);
    return PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents);
  }

  /**
   * Returns the validator workflow.
   *
   * <p>The validator workflow is independent of the regular ingestion workflow because validator
   * callbacks use validator-specific step types and transitions.
   */
  public Graph<StepType> validatorWorkflow() {
    return PipelinesWorkflow.getValidatorWorkflow();
  }

  /**
   * Returns {@code true} when event processing should be included in the resolved workflow.
   *
   * <p>A message is considered event-capable only when event processing is enabled globally, and
   * the dataset information says the dataset is a sampling-event dataset containing events.
   */
  private boolean containsEvents(PipelineBasedMessage message) {
    PipelineBasedMessage.DatasetInfo datasetInfo = message.getDatasetInfo();
    return config.eventsEnabled()
        && datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT
        && datasetInfo.isContainsEvents();
  }

  /** Returns whether the message says the dataset contains occurrence records. */
  private boolean containsOccurrences(PipelineBasedMessage message) {
    return message.getDatasetInfo().isContainsOccurrences();
  }
}
