package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.UUID;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.Test;

public class DwcaArchiveValidatorTest {

  @Test
  public void dwcaValidatorTest() {

    // State
    UUID key = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    ValidationWsClient validationClient =
        ValidationWsClientStub.builder().key(key).status(Status.RUNNING).build();
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = this.getClass().getResource("/dataset/dwca").getPath();

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setAttempt(1);
    message.setDatasetUuid(key);

    // When
    DwcaArchiveValidator.builder()
        .message(message)
        .config(config)
        .validationClient(validationClient)
        .schemaValidatorFactory(new SchemaValidatorFactory())
        .build()
        .validate();

    // Should
    Metrics metrics = validationClient.get(key).getMetrics();
    assertEquals(2, metrics.getFileInfos().size());

    FileInfo metadata = metrics.getFileInfos().get(0);
    assertEquals(DwcFileType.METADATA, metadata.getFileType());
    assertEquals("eml.xml", metadata.getFileName());

    FileInfo occurrence = metrics.getFileInfos().get(1);
    assertEquals(DwcFileType.CORE, occurrence.getFileType());
    assertEquals("occurrence.txt", occurrence.getFileName());
  }

  @Test
  public void createOutgoingMessageTest() throws Exception {
    // State
    UUID key = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = this.getClass().getResource("/dataset/dwca").getPath();
    config.validatorOnly = true;

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setAttempt(1);
    message.setDatasetUuid(key);
    message.setExecutionId(1L);

    // When
    PipelinesDwcaMessage result =
        DwcaArchiveValidator.builder()
            .message(message)
            .config(config)
            .build()
            .createOutgoingMessage();

    PipelinesDwcaMessage dValue =
        new ObjectMapper().readValue(result.toString(), PipelinesDwcaMessage.class);
    dValue.setPipelineSteps(null);

    // Should
    assertEquals(result.toString(), dValue.toString());
    assertEquals(key, result.getDatasetUuid());
    assertEquals(Integer.valueOf(1), result.getAttempt());
    assertEquals(new URI(config.stepConfig.registry.wsUrl), result.getSource());
    assertEquals(message.getExecutionId(), result.getExecutionId());
    assertEquals(DatasetType.OCCURRENCE, result.getDatasetType());
    assertEquals(EndpointType.DWC_ARCHIVE, result.getEndpointType());
    assertEquals(Platform.PIPELINES, result.getPlatform());
  }
}
