package org.gbif.pipelines.tasks.validators.checklist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.validator.checklist.ChecklistValidator;
import org.gbif.pipelines.validator.checklist.ChecklistbankWsClient;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.jupiter.api.Test;

public class ChecklistValidatorCallbackIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  @SneakyThrows
  public void checklistTest() {
    // State
    ChecklistValidatorConfiguration config = new ChecklistValidatorConfiguration();

    ChecklistbankWsClient checklistbankWsClientMock = new ChecklistbankWsClientMock();
    ChecklistValidator checklistValidator = new ChecklistValidator(checklistbankWsClientMock, null);

    ValidationWsClient validationClient = ValidationWsClientStub.create();
    MessagePublisher messagePublisher = MessagePublisherStub.create();

    UUID uuid = UUID.fromString("1e2e9421-6c68-4b78-b988-b6403deeb6dd");

    Validation validation = validationClient.get(uuid);
    validationClient.update(validation);

    ClbDatasetImport clbDatasetImport =
        OBJECT_MAPPER.readValue(
            ClassLoader.getSystemResourceAsStream(
                "checklists/api_response_without_extensions.json"),
            ClbDatasetImport.class);

    PipelinesChecklistValidatorMessage message =
        new PipelinesChecklistValidatorMessage(
            uuid, OBJECT_MAPPER.writeValueAsString(clbDatasetImport));

    // When
    ChecklistValidatorCallback callback =
        new ChecklistValidatorCallback(
            config, validationClient, messagePublisher, checklistValidator);
    callback.handleMessage(message);

    // Should
    Validation result = validationClient.get(uuid);
    assertEquals(1, result.getMetrics().getFileInfos().size());

    Optional<FileInfo> taxonOpt = getFileInfoByName(result, "taxon.txt");
    assertTrue(taxonOpt.isPresent());
    FileInfo taxon = taxonOpt.get();
    assertEquals(DwcFileType.CORE, taxon.getFileType());
    assertEquals(DwcTerm.Taxon.qualifiedName(), taxon.getRowType());
    assertEquals(Long.valueOf(20L), taxon.getCount());
    assertEquals(Long.valueOf(13L), taxon.getIndexedCount());
    assertEquals(5, taxon.getTerms().size());
    assertEquals(11, taxon.getIssues().size());
  }

  private Optional<FileInfo> getFileInfoByName(Validation validation, String name) {
    return validation.getMetrics().getFileInfos().stream()
        .filter(x -> x.getFileName().equals(name))
        .findAny();
  }
}
