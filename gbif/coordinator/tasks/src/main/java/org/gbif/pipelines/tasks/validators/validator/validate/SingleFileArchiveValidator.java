package org.gbif.pipelines.tasks.validators.validator.validate;

import java.net.URI;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class SingleFileArchiveValidator implements ArchiveValidator {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final PipelinesArchiveValidatorMessage message;

  @Override
  @SneakyThrows
  public PipelinesDwcaMessage createOutgoingMessage() {
    PipelinesDwcaMessage m = new PipelinesDwcaMessage();
    m.setDatasetUuid(message.getDatasetUuid());
    m.setAttempt(message.getAttempt());
    m.setSource(new URI(config.stepConfig.registry.wsUrl));
    m.setValidationReport(
        new DwcaValidationReport(
            message.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)));
    m.setPipelineSteps(message.getPipelineSteps());
    m.setExecutionId(message.getExecutionId());
    m.setDatasetType(DatasetType.OCCURRENCE);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    return m;
  }
}
