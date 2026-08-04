package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.tasks.validators.validator.validate.DatasetTypeUtils.getDatasetType;

import java.net.URI;
import java.util.*;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
public class DwcaArchiveValidator extends BaseDwcaArchiveValidator {

  @Builder
  public DwcaArchiveValidator(
      ArchiveValidatorConfiguration config,
      ValidationWsClient validationClient,
      SchemaValidatorFactory schemaValidatorFactory,
      PipelinesArchiveValidatorMessage message) {
    super(config, validationClient, schemaValidatorFactory, message);
  }

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
    getDatasetType(config.archiveRepository, message.getDatasetUuid()).ifPresent(m::setDatasetType);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    return m;
  }
}
