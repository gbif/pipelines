package org.gbif.pipelines.tasks;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * Common class for building and handling a validator step. This differs from the PipelinesCallback
 * in that it does not communicate with the registry to track the execution. Instead it calls the
 * validation ws.
 */
@Slf4j
@Builder
public class ValidatorCallback<I extends PipelineBasedMessage, O extends PipelineBasedMessage> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Retry RETRY =
      Retry.of(
          "validationCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .build());

  private static Properties properties;
  private final MessagePublisher publisher;
  @NonNull private final StepType stepType;
  private final DatasetClient datasetClient;
  @NonNull private final BaseConfiguration config;
  @NonNull private final I message;
  @NonNull private final StepHandler<I, O> handler;
  private final ValidationWsClient validationClient;

  static {
    try {
      properties = PropertiesUtil.loadProperties("pipelines.properties");
    } catch (IOException e) {
      log.error("Couldn't load pipelines properties", e);
    }
  }

  /**
   * The main process handling:
   *
   * <pre>
   *   1) Receives a MQ message
   *   2) Updates Zookeeper start date monitoring metrics
   *   4) Runs runnable function, which is the main message processing logic
   *   5) Updates Zookeeper end date monitoring metrics
   *   6) Update status in tracking service
   *   7) Sends a wrapped message to Balancer microservice
   *   8) Updates Zookeeper successful or error monitoring metrics
   *   9) Cleans Zookeeper monitoring metrics if the received message is the
   * last
   * </pre>
   */
  public void handleMessage() {

    String datasetKey = message.getDatasetUuid().toString();

    try (MDCCloseable mdc = MDC.putCloseable("datasetKey", datasetKey);
        MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc2 = MDC.putCloseable("step", stepType.name())) {

      if (!handler.isMessageCorrect(message) || isAborted()) {
        log.info(
            "Skip the message, please check that message is correct/runner/validation info/etc, exit from handler");
        return;
      }

      Validations.updateStatus(
          validationClient, message.getDatasetUuid(), stepType, Validation.Status.RUNNING);

      log.info("Message handler began - {}", message);
      Runnable runnable = handler.createRunnable(message);

      log.info("Handler has been started, datasetKey - {}", datasetKey);
      runnable.run();
      log.info("Handler has been finished, datasetKey - {}", datasetKey);

      Validations.updateStatus(
          validationClient,
          message.getDatasetUuid(),
          stepType,
          handler.getFinalValidationStatus(message));

      // Send a wrapped outgoing message to Balancer queue
      O outgoingMessage = handler.createOutgoingMessage(message);
      if (outgoingMessage != null) {

        // set the executionId
        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String logInfo =
            "Next message has been sent - "
                + outgoingMessage.getClass().getSimpleName()
                + ":"
                + outgoingMessage;
        log.info(logInfo);
      }
    } catch (Exception ex) {
      String error = "Error for datasetKey - " + datasetKey + " : " + ex.getMessage();
      log.error(error, ex);

      // update validator info
      String errorMessage = null;
      if (ex.getCause() instanceof PipelinesException) {
        errorMessage = ((PipelinesException) ex.getCause()).getShortMessage();
      }
      updateValidatorInfoStatus(Status.FAILED, errorMessage);
    }

    log.info("Message handler ended - {}", message);
  }

  private boolean isAborted() {
    Function<UUID, Validation> getValidationFn =
        key -> {
          log.info("Validation client: get validation by datasetKey {}", key);
          return validationClient.get(key);
        };
    Validation validation =
        Retry.decorateFunction(RETRY, getValidationFn).apply(message.getDatasetUuid());
    if (validation != null) {
      Status status = validation.getStatus();
      return status == Status.ABORTED || status == Status.FINISHED || status == Status.FAILED;
    } else {
      log.warn(
          "Can't find validation data key {}, please check that record exists",
          message.getDatasetUuid());
    }
    return false;
  }

  private void updateValidatorInfoStatus(Status status, String text) {
    Validations.updateStatus(validationClient, message.getDatasetUuid(), stepType, status, text);
  }
}
