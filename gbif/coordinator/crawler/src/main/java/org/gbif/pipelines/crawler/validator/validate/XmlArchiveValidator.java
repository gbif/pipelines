package org.gbif.pipelines.crawler.validator.validate;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.crawler.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.crawler.xml.XmlToAvroCallback;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class XmlArchiveValidator {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  @SneakyThrows
  public PipelinesXmlMessage createOutgoingMessage() {
    PipelinesXmlMessage m = new PipelinesXmlMessage();
    m.setDatasetUuid(message.getDatasetUuid());
    m.setAttempt(message.getAttempt());
    m.setValidator(config.validatorOnly);
    m.setPipelineSteps(message.getPipelineSteps());
    m.setEndpointType(EndpointType.BIOCASE_XML_ARCHIVE);
    m.setExecutionId(message.getExecutionId());
    m.setPlatform(Platform.PIPELINES);
    m.setReason(FinishReason.NORMAL);
    m.setTotalRecordCount(XmlToAvroCallback.SKIP_RECORDS_CHECK);
    return m;
  }

  public void validate() {
    Metrics metrics = Metrics.builder().build();

    // TODO: Validate XML files

    Validation validation = validationClient.get(message.getDatasetUuid());
    validation.setMetrics(metrics);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }
}
