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

import static org.gbif.common.messaging.api.messages.OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.tasks.client.RetryingDatasetClient;
import org.gbif.pipelines.tasks.client.RetryingPipelinesHistoryClient;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.pipelines.tasks.modes.CallbackMode;
import org.gbif.pipelines.tasks.modes.CallbackModeType;
import org.gbif.pipelines.tasks.modes.CallbackModes;
import org.gbif.pipelines.tasks.tracking.PipelinesStepTracker;
import org.gbif.pipelines.tasks.tracking.QueuedStepUpdater;
import org.gbif.pipelines.tasks.tracking.TrackingInfo;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * Orchestrates the lifecycle of a single pipeline callback message.
 *
 * <p>This class contains the common message-handling flow used by pipeline tasks. It does not
 * implement the task-specific processing itself; that responsibility belongs to the supplied {@link
 * StepHandler}. Instead, this class coordinates the surrounding infrastructure:
 *
 * <ul>
 *   <li>message validation through {@link StepHandler#isMessageCorrect(PipelineBasedMessage)}
 *   <li>pipeline or validator mode behavior through {@link CallbackMode}
 *   <li>pipeline step tracking through {@link PipelinesStepTracker}
 *   <li>dataset-deletion checks before and after task execution
 *   <li>publishing the next message to the balancer queue
 *   <li>marking queued steps for downstream execution
 *   <li>marking the pipeline execution as finished when all steps are complete
 * </ul>
 *
 * <p>The class is generic over input and output message types:
 *
 * <ul>
 *   <li>{@code In} is the message consumed by the current step
 *   <li>{@code Out} is the message produced for the next step
 * </ul>
 *
 * <p>Different callback modes are selected through {@link CallbackModeType}. Regular ingestion uses
 * the pipeline mode, while validator callbacks use validator-specific behavior without spreading
 * validator conditionals through this class.
 */
@Slf4j
@Builder
public class PipelinesCallback<In extends PipelineBasedMessage, Out extends PipelineBasedMessage> {

  private static Properties properties;
  private final MessagePublisher publisher;
  @NonNull private final StepType stepType;
  @NonNull private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  @NonNull private final BaseConfiguration config;
  @NonNull private final In message;
  @NonNull private final StepHandler<In, Out> handler;
  private final ValidationWsClient validationClient;
  @Builder.Default private final CallbackModeType callbackModeType = CallbackModeType.PIPELINES;

  static {
    try {
      properties = PropertiesUtil.loadProperties("pipelines.properties");
    } catch (IOException e) {
      log.error("Couldn't load pipelines properties", e);
    }
  }

  private CallbackMode callbackMode() {
    return CallbackModes.from(callbackModeType);
  }

  /**
   * Builds the internal callback context used by mode and helper classes.
   *
   * <p>The context groups the current message, callback configuration, clients, and helper services
   * required during message processing. It keeps {@link CallbackMode} implementations independent
   * from {@link PipelinesCallback} internals, so modes do not need to call package-private methods
   * on this class.
   */
  private PipelinesCallbackContext<In> context() {
    RetryingPipelinesHistoryClient retryingHistoryClient =
        new RetryingPipelinesHistoryClient(historyClient);
    RetryingDatasetClient retryingDatasetClient =
        datasetClient == null ? null : new RetryingDatasetClient(datasetClient);
    RetryingValidationClient retryingValidationClient =
        validationClient == null ? null : new RetryingValidationClient(validationClient);

    PipelinesStepTracker tracker = new PipelinesStepTracker(retryingHistoryClient, config);
    PipelinesExecutionGuard executionGuard = new PipelinesExecutionGuard(historyClient);
    ValidatorStatusService validatorStatusService =
        new ValidatorStatusService(retryingValidationClient);
    QueuedStepUpdater queuedStepUpdater = new QueuedStepUpdater(retryingHistoryClient);
    WorkflowResolver workflowResolver = new WorkflowResolver(config);

    return PipelinesCallbackContext.<In>builder()
        .message(message)
        .stepType(stepType)
        .config(config)
        .publisher(publisher)
        .historyClient(historyClient)
        .retryingHistoryClient(retryingHistoryClient)
        .datasetClient(datasetClient)
        .retryingDatasetClient(retryingDatasetClient)
        .validationClient(validationClient)
        .retryingValidationClient(retryingValidationClient)
        .tracker(tracker)
        .executionGuard(executionGuard)
        .validatorStatusService(validatorStatusService)
        .queuedStepUpdater(queuedStepUpdater)
        .workflowResolver(workflowResolver)
        .build();
  }

  /**
   * Handles the configured input message.
   *
   * <p>The processing flow is:
   *
   * <ol>
   *   <li>Attach dataset, attempt, and step information to the logging context.
   *   <li>Skip the message if it is invalid or if the selected callback mode says processing should
   *       stop.
   *   <li>Track the pipeline step when the selected mode requires tracking.
   *   <li>Create and run the task-specific {@link Runnable} from the configured {@link
   *       StepHandler}.
   *   <li>Update tracking status to {@link PipelineStep.Status#COMPLETED} after successful
   *       processing.
   *   <li>Create and publish the outgoing message for the next pipeline step if one exists.
   *   <li>Update queued status for downstream steps.
   *   <li>Notify the selected callback mode about success or failure.
   *   <li>Ask the history service to mark the execution as finished if all steps are finished.
   * </ol>
   *
   * <p>Any exception thrown by task execution or infrastructure calls is caught, logged, reflected
   * in tracking status when possible, and forwarded to the callback mode for mode-specific failure
   * handling.
   */
  public void handleMessage() {
    String datasetKey = message.getDatasetUuid().toString();
    Optional<TrackingInfo> info = Optional.empty();
    PipelinesCallbackContext<In> context = context();
    CallbackMode callbackMode = callbackMode();

    try (MDCCloseable datasetMdc = MDC.putCloseable("datasetKey", datasetKey);
        MDCCloseable attemptMdc = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable stepMdc = MDC.putCloseable("step", stepType.name())) {

      if (!handler.isMessageCorrect(message) || callbackMode.shouldSkip(context)) {
        log.info(
            "Skip the message, please check that message is correct/runner/validation info/etc, exit from handler");
        return;
      }

      info = callbackMode.trackPipelineStep(context);

      log.info("Message handler began - {}", message);
      Runnable runnable = handler.createRunnable(message);

      log.info("Handler has been started, datasetKey - {}", datasetKey);
      checkIfDatasetIsDeleted(context);
      runnable.run();
      checkIfDatasetIsDeleted(context);
      log.info("Handler has been finished, datasetKey - {}", datasetKey);

      context.getTracker().updateStatus(info, PipelineStep.Status.COMPLETED);

      Out outgoingMessage = handler.createOutgoingMessage(message);
      if (outgoingMessage != null) {
        info.ifPresent(i -> outgoingMessage.setExecutionId(i.getExecutionId()));

        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        log.info(
            "Next message has been sent - {}:{}",
            outgoingMessage.getClass().getSimpleName(),
            outgoingMessage);

        callbackMode.updateQueuedStatus(info, context);
      }

      callbackMode.onSuccess(context);
    } catch (Exception ex) {
      String error = "Error for datasetKey - " + datasetKey + " : " + ex.getMessage();
      log.error(error, ex);

      context.getTracker().updateStatus(info, PipelineStep.Status.FAILED);

      String errorMessage = null;
      if (ex.getCause() instanceof PipelinesException) {
        errorMessage = ((PipelinesException) ex.getCause()).getShortMessage();
      }
      callbackMode.onFailure(context, errorMessage);
    } finally {
      if (message.getExecutionId() != null) {
        log.info("Mark execution as FINISHED if all steps are FINISHED");
        context
            .getRetryingHistoryClient()
            .markPipelineExecutionIfFinished(message.getExecutionId());
      }
    }

    log.info("Message handler ended - {}", message);
  }

  /**
   * Returns the pipeline version loaded from {@code pipelines.properties}.
   *
   * <p>The version is stored with pipeline step tracking information to make processed records
   * traceable to the application version that produced them.
   */
  @VisibleForTesting
  public static String getPipelinesVersion() {
    return properties == null ? null : properties.getProperty("pipelines.version");
  }

  /**
   * Stops processing when the dataset has been deleted while the pipeline step is running.
   *
   * <p>The check is performed before and after the task-specific runnable. If the registry marks
   * the dataset as deleted, a delete-occurrences message is published and processing fails with a
   * {@link PipelinesException}. This prevents continuing ingestion for data that should no longer
   * be indexed.
   *
   * <p>If no dataset client is configured, the check is skipped.
   */
  private void checkIfDatasetIsDeleted(PipelinesCallbackContext<In> context) throws IOException {
    RetryingDatasetClient retryingDatasetClient = context.getRetryingDatasetClient();
    if (retryingDatasetClient != null) {
      Dataset dataset = retryingDatasetClient.get(message.getDatasetUuid());
      if (dataset != null && dataset.getDeleted() != null) {
        publisher.send(
            new DeleteDatasetOccurrencesMessage(message.getDatasetUuid(), NOT_SEEN_IN_LAST_CRAWL));
        log.error("The dataset marked as deleted while was being in the processing");
        throw new PipelinesException("The dataset marked as deleted");
      }
    } else {
      log.warn("datasetClient object is null, skip checkIfDatasetIsDeleted");
    }
  }
}
