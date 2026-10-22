package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.UUID;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationType;
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
  public void samplingEventDwcaValidatorTest() {

    // State
    UUID key = UUID.fromString("a731e3b9-1ff9-4b22-a7e8-7d40aced5bc7");
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

    // Should: METADATA + Event CORE + Occurrence EXTENSION
    Metrics metrics = validationClient.get(key).getMetrics();
    assertEquals(3, metrics.getFileInfos().size());

    FileInfo metadata = metrics.getFileInfos().get(0);
    assertEquals(DwcFileType.METADATA, metadata.getFileType());
    assertEquals("eml.xml", metadata.getFileName());

    FileInfo eventCore = metrics.getFileInfos().get(1);
    assertEquals(DwcFileType.CORE, eventCore.getFileType());
    assertEquals(DwcTerm.Event.qualifiedName(), eventCore.getRowType());
    assertEquals("event.txt", eventCore.getFileName());

    FileInfo occurrenceExtension = metrics.getFileInfos().get(2);
    assertEquals(DwcFileType.EXTENSION, occurrenceExtension.getFileType());
    assertEquals(DwcTerm.Occurrence.qualifiedName(), occurrenceExtension.getRowType());
    assertEquals("occurrence.txt", occurrenceExtension.getFileName());
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
        (PipelinesDwcaMessage)
            DwcaArchiveValidator.builder()
                .message(message)
                .config(config)
                .build()
                .createOutgoingMessage();

    PipelinesDwcaMessage dValue =
        new ObjectMapper().readValue(result.toString(), PipelinesDwcaMessage.class);
    dValue.setPipelineSteps(null);
    dValue.setPlatform(null);

    // Should
    assertEquals(result.toString(), dValue.toString());
    assertEquals(key, result.getDatasetUuid());
    assertEquals(Integer.valueOf(1), result.getAttempt());
    assertEquals(new URI(config.stepConfig.registry.wsUrl), result.getSource());
    assertEquals(message.getExecutionId(), result.getExecutionId());
    assertEquals(DatasetType.OCCURRENCE, result.getDatasetType());
    assertEquals(EndpointType.DWC_ARCHIVE, result.getEndpointType());
  }

  @Test
  public void emptyOccurrenceCoreFailsTest() {
    assertFailsWith(
        "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0001",
        "occurrence.txt",
        EvaluationType.OCCURRENCE_CORE_EMPTY);
  }

  @Test
  public void missingOccurrenceExtensionFailsTest() {
    assertFailsWith(
        "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0002",
        "event.txt",
        EvaluationType.OCCURRENCE_EXTENSION_MISSING);
  }

  @Test
  public void emptyOccurrenceExtensionFailsTest() {
    assertFailsWith(
        "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0003",
        "occurrence.txt",
        EvaluationType.OCCURRENCE_EXTENSION_EMPTY);
  }

  @Test
  public void noOccurrencesPassesWhenCheckDisabledTest() {
    for (String k :
        new String[] {
          "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0001",
          "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0002",
          "1c1b4c6e-0b7a-4a4e-9a3e-2f6b1d1e0003"
        }) {
      newValidator(UUID.fromString(k), false).validate();
    }
  }

  @Test
  public void archivesWithOccurrencesPassWhenCheckEnabledTest() {
    newValidator(UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad"), true).validate();
    newValidator(UUID.fromString("a731e3b9-1ff9-4b22-a7e8-7d40aced5bc7"), true).validate();
  }

  private void assertFailsWith(String datasetKey, String fileName, EvaluationType expected) {
    // State
    UUID key = UUID.fromString(datasetKey);
    ValidationWsClient validationClient =
        ValidationWsClientStub.builder().key(key).status(Status.RUNNING).build();

    // When
    DwcaArchiveValidator validator = newValidator(key, true, validationClient);

    // Should
    assertThrows(IllegalArgumentException.class, validator::validate);
    FileInfo fileInfo =
        validationClient.get(key).getMetrics().getFileInfos().stream()
            .filter(fi -> fileName.equals(fi.getFileName()))
            .findFirst()
            .orElseThrow();
    assertTrue(fileInfo.getIssues().stream().anyMatch(i -> expected.name().equals(i.getIssue())));
  }

  private DwcaArchiveValidator newValidator(UUID key, boolean failOnNoOccurrences) {
    return newValidator(
        key,
        failOnNoOccurrences,
        ValidationWsClientStub.builder().key(key).status(Status.RUNNING).build());
  }

  private DwcaArchiveValidator newValidator(
      UUID key, boolean validatorOnly, ValidationWsClient validationClient) {
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = this.getClass().getResource("/dataset/dwca").getPath();
    config.validatorOnly = validatorOnly;

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setAttempt(1);
    message.setDatasetUuid(key);

    return DwcaArchiveValidator.builder()
        .message(message)
        .config(config)
        .validationClient(validationClient)
        .schemaValidatorFactory(new SchemaValidatorFactory())
        .build();
  }
}
