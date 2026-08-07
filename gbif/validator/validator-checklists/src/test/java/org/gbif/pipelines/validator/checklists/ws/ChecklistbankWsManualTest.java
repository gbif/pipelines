package org.gbif.pipelines.validator.checklists.ws;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.gbif.pipelines.validator.checklists.ChecklistValidator;
import org.gbif.validator.api.Metrics;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ChecklistbankWsManualTest {

  @Test
  public void wsManualTest() throws IOException {
    ChecklistValidator checklistValidator =
        new ChecklistValidator("https://api.dev.checklistbank.org", "user", "pwd");

    List<Metrics.FileInfo> report =
        checklistValidator.evaluate(
            Paths.get(
                ClassLoader.getSystemResource("checklists/archive_without_extensions.zip")
                    .getFile()));

    assertEquals(1, report.size());
  }
}
