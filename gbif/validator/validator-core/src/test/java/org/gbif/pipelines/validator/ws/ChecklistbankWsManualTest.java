package org.gbif.pipelines.validator.ws;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ChecklistbankWsManualTest {

  @Test
  public void wsManualTest() throws IOException {
    ChecklistValidator checklistValidator =
        new ChecklistValidator(
            "https://api.dev.checklistbank.org", "user", "pwd", "http://test.com");

    int datasetKey =
        checklistValidator
            .submitValidation(
                Paths.get(
                    ClassLoader.getSystemResource("checklists/archive_without_extensions.zip")
                        .getFile()),
                UUID.randomUUID())
            .join();

    Assertions.assertTrue(datasetKey > 0);
  }
}
