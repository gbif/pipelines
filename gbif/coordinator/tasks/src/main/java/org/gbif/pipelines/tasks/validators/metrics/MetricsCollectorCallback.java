package org.gbif.pipelines.tasks.validators.metrics;

import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesMetricsCollectedMessage;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.validators.metrics.collector.MetricsCollectorFactory;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesIndexedMessage} is received. */
@Slf4j
@AllArgsConstructor
public class MetricsCollectorCallback extends AbstractMessageCallback<PipelinesIndexedMessage>
    implements StepHandler<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage> {

  private static final StepType TYPE = StepType.VALIDATOR_COLLECT_METRICS;

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;

  @Override
  public void handleMessage(PipelinesIndexedMessage message) {
    PipelinesCallback.<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(TYPE)
        .isValidator(config.validatorOnly)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    PipelinesIndexedMessage message = new PipelinesIndexedMessage();
    if (config.validatorOnly) {
      message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_COLLECT_METRICS.name()));
    }
    return message.getRoutingKey() + ".*";
  }

  @Override
  public boolean isMessageCorrect(PipelinesIndexedMessage message) {
    return message.getDatasetUuid() != null && message.getEndpointType() != null;
  }

  @Override
  public Runnable createRunnable(PipelinesIndexedMessage message) {
    return () -> {
      log.info("Running metrics collector for {}", message.getDatasetUuid());
      MetricsCollectorFactory.builder()
          .config(config)
          .publisher(publisher)
          .validationClient(validationClient)
          .message(message)
          .stepType(TYPE)
          .build()
          .create()
          .collect();
    };
  }
}
