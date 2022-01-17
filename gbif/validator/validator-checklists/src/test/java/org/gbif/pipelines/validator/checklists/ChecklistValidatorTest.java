package org.gbif.pipelines.validator.checklists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.junit.Test;

/** Unit tests related to {@link org.gbif.pipelines.validator.checklists.ChecklistValidator}. */
public class ChecklistValidatorTest {

  @Test
  public void testChecklistValidator() {

    Path temp = Paths.get(getClass().getResource("/").getFile());

    // State
    NeoConfiguration neoConfiguration = new NeoConfiguration();
    neoConfiguration.neoRepository = temp.resolve("neo").toFile();

    try (ChecklistValidator checklistValidator =
        new ChecklistValidator(
            ChecklistValidator.Configuration.builder()
                .neoConfiguration(neoConfiguration)
                .build())) {

      // When
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              Paths.get(
                  ClassLoader.getSystemResource("checklists/00000001-c6af-11e2-9b88-00145eb45e9a/")
                      .getFile()));

      // Should
      // Metrics.FileInfo checks
      assertEquals(1, report.size());
      assertEquals(Long.valueOf(20), report.get(0).getCount());
      assertEquals(Long.valueOf(20), report.get(0).getIndexedCount());
      assertEquals(DwcFileType.CORE, report.get(0).getFileType());
      assertEquals(DwcTerm.Taxon.qualifiedName(), report.get(0).getRowType());
      assertEquals("taxa.txt", report.get(0).getFileName());

      // Metrics.IssueInfo checks
      assertEquals(1, report.get(0).getIssues().size());
      assertEquals(Long.valueOf(20), report.get(0).getIssues().get(0).getCount());
      assertEquals(
          NameUsageIssue.BACKBONE_MATCH_NONE.name(), report.get(0).getIssues().get(0).getIssue());
      assertEquals(5, report.get(0).getIssues().get(0).getSamples().size());
      assertEquals(
          EvaluationCategory.CLB_INTERPRETATION_BASED,
          report.get(0).getIssues().get(0).getIssueCategory());

      assertFalse(report.get(0).getTerms().isEmpty());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
