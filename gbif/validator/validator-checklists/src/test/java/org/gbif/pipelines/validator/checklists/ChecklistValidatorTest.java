package org.gbif.pipelines.validator.checklists;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.validator.api.Metrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests related to {@link org.gbif.pipelines.validator.checklists.ChecklistValidator}. */
public class ChecklistValidatorTest {

  @TempDir static Path folder;

  @Test
  public void testChecklistEvaluator() {
    NeoConfiguration neoConfiguration = new NeoConfiguration();
    neoConfiguration.neoRepository = folder.resolve("neo").toFile();
    ChecklistValidator checklistValidator = new ChecklistValidator(neoConfiguration);
    try {
      Metrics.ChecklistValidationReport report =
          checklistValidator.evaluate(
              Paths.get(
                  ClassLoader.getSystemResource("checklists/00000001-c6af-11e2-9b88-00145eb45e9a/")
                      .getFile()));
      assertEquals(20, report.getResults().size());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
