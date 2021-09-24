package org.gbif.pipelines.validator.checklists;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
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
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              Paths.get(
                  ClassLoader.getSystemResource("checklists/00000001-c6af-11e2-9b88-00145eb45e9a/")
                      .getFile()));

      // Metrics.FileInfo checks
      assertEquals(report.size(), 1);
      assertEquals(report.get(0).getCount(), 20);
      assertEquals(report.get(0).getFileType(), DwcFileType.CORE);
      assertEquals(report.get(0).getRowType(), DwcTerm.Taxon.simpleName());
      assertEquals(report.get(0).getFileName(), "taxa.txt");

      // Metrics.IssueInfo checks
      assertEquals(report.get(0).getIssues().size(), 1);
      assertEquals(report.get(0).getIssues().get(0).getCount(), 20);
      assertEquals(
          report.get(0).getIssues().get(0).getIssue(), NameUsageIssue.BACKBONE_MATCH_NONE.name());
      assertEquals(report.get(0).getIssues().get(0).getSamples().size(), 5);
      assertEquals(
          report.get(0).getIssues().get(0).getIssueCategory(),
          EvaluationCategory.CLB_INTERPRETATION_BASED);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
