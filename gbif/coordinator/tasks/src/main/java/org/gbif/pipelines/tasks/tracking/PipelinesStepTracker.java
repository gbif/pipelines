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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesRunnerMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.client.RetryingPipelinesHistoryClient;

/**
 * Creates and updates pipeline tracking records in the history service.
 *
 * <p>This class owns the tracking-related part of callback processing. It is responsible for:
 *
 * <ul>
 *   <li>creating or reusing a pipeline process for a dataset attempt
 *   <li>creating an initial pipeline execution when the incoming message does not have one
 *   <li>marking the current step as {@link PipelineStep.Status#RUNNING}
 *   <li>storing the incoming message payload on the tracked step
 *   <li>updating the step status and metrics after processing
 * </ul>
 *
 * <p>Tracking failures are logged but do not abort callback processing. This mirrors the historical
 * pipeline behavior: tracking is important for observability, but a temporary tracking failure
 * should not necessarily stop data processing.
 */
@Slf4j
@RequiredArgsConstructor
public class PipelinesStepTracker {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RetryingPipelinesHistoryClient historyClient;
  private final BaseConfiguration config;

  /**
   * Tracks the start of a pipeline step.
   *
   * <p>If the message does not already contain an execution id, this method creates an initial
   * pipeline execution using the workflow that starts from {@code stepType}. It then finds the
   * corresponding step in the execution, verifies that the step has not already been processed, and
   * marks it as {@link PipelineStep.Status#RUNNING}.
   *
   * <p>The returned {@link TrackingInfo} contains the identifiers needed later to update the same
   * step and to queue downstream steps.
   *
   * @return tracking information when tracking succeeds; {@link Optional#empty()} when tracking
   *     fails
   */
  public Optional<TrackingInfo> track(PipelineBasedMessage message, StepType stepType) {
    try {
      UUID datasetUuid = message.getDatasetUuid();
      Integer attempt = message.getAttempt();

      long processKey = historyClient.createPipelineProcess(datasetUuid, attempt);

      Long executionId = ensureExecution(message, stepType, processKey);
      List<PipelineStep> stepsByExecutionKey =
          historyClient.getPipelineStepsByExecutionKey(executionId);

      PipelineStep step =
          stepsByExecutionKey.stream()
              .filter(ps -> ps.getType() == stepType)
              .findAny()
              .orElseThrow(
                  () ->
                      new PipelinesException(
                          "History service doesn't contain stepType: " + stepType));

      if (PipelinesStepSets.isProcessedStep(step)) {
        log.error(
            "Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}",
            datasetUuid);
        throw new PipelinesException(
            "Dataset is in the queue, please check the pipeline-ingestion monitoring tool");
      }

      step.setMessage(OBJECT_MAPPER.writeValueAsString(message))
          .setState(PipelineStep.Status.RUNNING)
          .setRunner(StepRunner.valueOf(getRunner(message)))
          .setStarted(OffsetDateTime.now())
          .setPipelinesVersion(PipelinesCallback.getPipelinesVersion());

      long stepKey = historyClient.saveStep(step);

      Map<StepType, PipelineStep> pipelineStepMap =
          stepsByExecutionKey.stream()
              .collect(Collectors.toMap(PipelineStep::getType, Function.identity()));

      return Optional.of(
          TrackingInfo.builder()
              .processKey(processKey)
              .executionId(executionId)
              .pipelineStepMap(pipelineStepMap)
              .stepKey(stepKey)
              .datasetId(datasetUuid.toString())
              .attempt(attempt.toString())
              .build());

    } catch (Exception ex) {
      log.error("Couldn't track pipeline step for message {}", message, ex);
      return Optional.empty();
    }
  }

  /**
   * Updates the tracked pipeline step after processing.
   *
   * <p>The update includes:
   *
   * <ul>
   *   <li>the new step status
   *   <li>metrics read from the configured metadata file
   *   <li>record counts derived from metrics when possible
   *   <li>the finish timestamp for terminal successful/aborted statuses
   * </ul>
   *
   * <p>If the step is already in a finished state in the history service, this method leaves it
   * unchanged. That protects terminal states from being overwritten by late or duplicate callback
   * updates.
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public void updateStatus(Optional<TrackingInfo> trackingInfoOpt, PipelineStep.Status status) {
    if (trackingInfoOpt.isEmpty()) {
      return;
    }
    TrackingInfo trackingInfo = trackingInfoOpt.get();
    String path =
        HdfsUtils.buildOutputPathAsString(
            config.getRepositoryPath(),
            trackingInfo.getDatasetId(),
            trackingInfo.getAttempt(),
            config.getMetaFileName());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());

    List<MetricInfo> metricInfos = HdfsUtils.readMetricsFromMetaFile(hdfsConfigs, path);

    PipelineStep pipelineStep = historyClient.getPipelineStep(trackingInfo.getStepKey());

    pipelineStep.setState(status);
    pipelineStep.setMetrics(new HashSet<>(metricInfos));

    if (metricInfos.size() == 1) {
      Optional.ofNullable(metricInfos.get(0).getValue())
          .filter(v -> !v.isEmpty())
          .map(Long::parseLong)
          .ifPresent(pipelineStep::setNumberRecords);
    } else if (metricInfos.size() > 1) {
      pipelineStep.setNumberRecords(-1L);
    }

    if (status == PipelineStep.Status.COMPLETED || status == PipelineStep.Status.ABORTED) {
      pipelineStep.setFinished(OffsetDateTime.now());
    }

    try {
      long stepKey = historyClient.updateStepUnlessFinished(pipelineStep);

      log.info(
          "Step key {}, step type {} is {}",
          stepKey,
          pipelineStep.getType(),
          pipelineStep.getState());

    } catch (Exception ex) {
      log.error(
          "Couldn't update tracking status for process {} and step {}",
          trackingInfo.getProcessKey(),
          trackingInfo.getStepKey(),
          ex);
    }
  }

  /**
   * Returns the message execution id, creating the initial execution when needed.
   *
   * <p>A missing execution id means this message starts a new pipeline execution. In that case the
   * workflow is resolved from the message dataset information and the current step type, then
   * stored in the history service. The newly created execution id is written back to the message so
   * downstream messages can continue the same execution.
   */
  private Long ensureExecution(PipelineBasedMessage message, StepType stepType, long processKey) {
    Long executionId = message.getExecutionId();
    if (executionId != null) {
      return executionId;
    }

    log.info("executionId is empty, create initial pipelines execution");

    boolean containsEvents = containsEvents(message);
    boolean containsOccurrences = message.getDatasetInfo().isContainsOccurrences();

    Set<StepType> stepTypes =
        PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
            .getAllNodesFor(Collections.singleton(stepType));

    PipelineExecution execution =
        new PipelineExecution().setStepsToRun(stepTypes).setCreated(OffsetDateTime.now());

    executionId = historyClient.addPipelineExecution(processKey, execution);

    message.setExecutionId(executionId);
    return executionId;
  }

  /**
   * Returns whether event steps should be included when creating a new pipeline execution.
   *
   * <p>Event processing is enabled only when the application configuration allows event pipelines
   * and the message describes a sampling-event dataset containing event records.
   */
  private boolean containsEvents(PipelineBasedMessage message) {
    PipelineBasedMessage.DatasetInfo datasetInfo = message.getDatasetInfo();
    return config.eventsEnabled()
        && datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT
        && datasetInfo.isContainsEvents();
  }

  /**
   * Resolves the runner to store on the tracked pipeline step.
   *
   * <p>Archive conversion messages are handled by standalone runners. Runner messages carry their
   * runner explicitly. Unknown message types are tracked as {@link StepRunner#UNKNOWN}.
   */
  private String getRunner(PipelineBasedMessage message) {
    if (message instanceof PipelinesAbcdMessage
        || message instanceof PipelinesXmlMessage
        || message instanceof PipelinesDwcaMessage) {
      return StepRunner.STANDALONE.name();
    }
    if (message instanceof PipelinesRunnerMessage) {
      return ((PipelinesRunnerMessage) message).getRunner();
    }
    return StepRunner.UNKNOWN.name();
  }
}
