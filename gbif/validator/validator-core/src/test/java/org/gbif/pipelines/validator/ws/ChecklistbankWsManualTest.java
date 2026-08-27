package org.gbif.pipelines.validator.ws;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ChecklistbankWsManualTest {

  private static final String DEV_API_URL = "https://api.dev.checklistbank.org";
  private static final String USER = "user";
  private static final String PWD = "pwd";
  private static final String CALLBACK = "http://test.com";

  @Test
  public void manualValidationTest() throws IOException {
    ChecklistValidator checklistValidator =
        new ChecklistValidator(DEV_API_URL, USER, PWD, CALLBACK);

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

  @Test
  public void manualCheckImport() {
    ChecklistbankWsClient checklistbankWsClient =
        new ClientBuilder()
            .withUrl(DEV_API_URL)
            .withCredentials(USER, PWD)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getDefaultObjectMapper())
            .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
            .build(ChecklistbankWsClient.class);

    List<ChecklistbankWsClient.ImportResponse> responseList =
        checklistbankWsClient.checkImport(100000197);

    Assertions.assertEquals(1, responseList.size());
  }
}
