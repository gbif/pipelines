package org.gbif.pipelines.tasks.validators.validator.validate;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallback;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class XmlArchiveValidator implements ArchiveValidator {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final PipelinesArchiveValidatorMessage message;

  @Override
  @SneakyThrows
  public PipelinesXmlMessage createOutgoingMessage() {
    PipelinesXmlMessage m = new PipelinesXmlMessage();
    m.setDatasetUuid(message.getDatasetUuid());
    m.setAttempt(message.getAttempt());
    m.setPipelineSteps(message.getPipelineSteps());
    m.setEndpointType(EndpointType.BIOCASE_XML_ARCHIVE);
    m.setExecutionId(message.getExecutionId());
    m.setReason(FinishReason.NORMAL);
    m.setTotalRecordCount(XmlToAvroCallback.SKIP_RECORDS_CHECK);
    return m;
  }
}
