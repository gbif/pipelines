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
package org.gbif.pipelines.tasks.client;

import io.github.resilience4j.retry.Retry;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.pipelines.core.factory.RetryFactory;
import org.gbif.pipelines.tasks.tracking.PipelinesStepSets;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

@Slf4j
@RequiredArgsConstructor
public class RetryingPipelinesHistoryClient {

  private static final Retry RETRY = RetryFactory.create(null, "registryCall");

  private final PipelinesHistoryClient historyClient;

  public long createPipelineProcess(UUID datasetUuid, Integer attempt) {
    return Retry.decorateSupplier(
            RETRY,
            () -> {
              log.info(
                  "History client: create pipeline process, datasetKey {}, attempt {}",
                  datasetUuid,
                  attempt);
              return historyClient.createPipelineProcess(
                  new PipelineProcessParameters(datasetUuid, attempt));
            })
        .get();
  }

  public long addPipelineExecution(long processKey, PipelineExecution execution) {
    return Retry.decorateSupplier(
            RETRY,
            () -> {
              log.info(
                  "History client: add pipeline execution, processKey {}, execution {}",
                  processKey,
                  execution);
              return historyClient.addPipelineExecution(processKey, execution);
            })
        .get();
  }

  public List<PipelineStep> getPipelineStepsByExecutionKey(Long executionKey) {
    return Retry.decorateFunction(
            RETRY,
            (Long key) -> {
              log.info("History client: get steps by execution key {}", key);
              return historyClient.getPipelineStepsByExecutionKey(key);
            })
        .apply(executionKey);
  }

  public PipelineStep getPipelineStep(Long stepKey) {
    return Retry.decorateFunction(
            RETRY,
            (Long key) -> {
              log.info("History client: get step by key {}", key);
              return historyClient.getPipelineStep(key);
            })
        .apply(stepKey);
  }

  public long saveStep(PipelineStep step) {
    return Retry.decorateFunction(
            RETRY,
            (PipelineStep stepToSave) -> {
              log.info("History client: update pipeline step: {}", stepToSave);
              return historyClient.updatePipelineStep(stepToSave);
            })
        .apply(step);
  }

  public long updateStepUnlessFinished(PipelineStep step) {
    return Retry.decorateFunction(RETRY, this::updateStepUnlessFinishedWithoutRetry).apply(step);
  }

  private long updateStepUnlessFinishedWithoutRetry(PipelineStep step) {
    log.info("History client: update pipeline step: {}", step);

    PipelineStep currentStep = historyClient.getPipelineStep(step.getKey());
    if (PipelinesStepSets.isFinishedStep(currentStep.getState())) {
      return currentStep.getKey();
    }

    return historyClient.updatePipelineStep(step);
  }

  public void setSubmittedPipelineStepToQueued(long stepKey) {
    Retry.decorateRunnable(RETRY, () -> historyClient.setSubmittedPipelineStepToQueued(stepKey))
        .run();
  }

  public void markPipelineExecutionIfFinished(Long executionId) {
    Retry.decorateRunnable(RETRY, () -> historyClient.markPipelineExecutionIfFinished(executionId))
        .run();
  }
}
