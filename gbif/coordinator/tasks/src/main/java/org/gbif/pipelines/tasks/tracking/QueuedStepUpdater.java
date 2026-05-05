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
package org.gbif.pipelines.tasks.tracking;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.tasks.client.RetryingPipelinesHistoryClient;

/**
 * Marks downstream pipeline steps as {@link PipelineStep.Status#QUEUED} in the history service
 * after a step's outgoing message has been published.
 *
 * <p>The update uses the workflow graph to find the direct successors of the completed step and
 * transitions only those that are not already in a processed state, preventing accidental
 * overwriting of steps that were completed by a concurrent execution.
 */
@Slf4j
@RequiredArgsConstructor
public class QueuedStepUpdater {

  private final RetryingPipelinesHistoryClient historyClient;

  /**
   * Marks the immediate successors of {@code stepType} in {@code workflow} as {@link
   * PipelineStep.Status#QUEUED}. Only steps present in the {@link TrackingInfo#getPipelineStepMap()
   * step map} and not yet in a processed state are updated.
   */
  public void updateQueuedStatus(TrackingInfo info, StepType stepType, Graph<StepType> workflow) {
    List<Graph<StepType>.Edge> nodeEdges = workflow.getNodeEdges(stepType);

    for (Graph<StepType>.Edge edge : nodeEdges) {
      PipelineStep step = info.getPipelineStepMap().get(edge.getNode());
      if (step != null && PipelinesStepSets.isNotProcessedStep(step.getState())) {
        log.info("History client: set pipeline step to QUEUED: {}", step);
        historyClient.setSubmittedPipelineStepToQueued(step.getKey());
        log.info("Step {} with step key {} as QUEUED", step.getType(), step.getKey());
      }
    }
  }
}
