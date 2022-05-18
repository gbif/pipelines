package org.gbif.pipelines.tasks.validators.metrics.collector;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DefaultMetricsCollector implements MetricsCollector {

  private final ValidationWsClient validationClient;
  private final PipelinesIndexedMessage message;

  @Override
  public void collect() {
    log.info("Endpoint type {} is not supported!", message.getEndpointType());
    Validation validation = validationClient.get(message.getDatasetUuid());
    validation.setStatus(Status.FAILED);
    validationClient.update(validation);
  }
}
