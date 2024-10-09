package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.junit.Test;

public class XmlArchiveValidatorTest {

  @Test
  public void createOutgoingMessageTest() {
    // State
    UUID key = UUID.fromString("7ef15372-1387-11e2-bb2e-00145eb45e9a");
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = this.getClass().getResource("/dataset/xml").getPath();
    config.validatorOnly = true;

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setAttempt(1);
    message.setDatasetUuid(key);
    message.setExecutionId(1L);

    // When
    PipelinesXmlMessage result =
        XmlArchiveValidator.builder()
            .message(message)
            .config(config)
            .build()
            .createOutgoingMessage();

    // Should
    assertEquals(key, result.getDatasetUuid());
    assertEquals(Integer.valueOf(1), result.getAttempt());
    assertEquals(message.getExecutionId(), result.getExecutionId());
    assertEquals(FinishReason.NORMAL, result.getReason());
    assertEquals(EndpointType.BIOCASE_XML_ARCHIVE, result.getEndpointType());
  }
}
