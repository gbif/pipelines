package org.gbif.pipelines.validator.checklists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Paths;
import java.util.List;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.validator.checklists.ws.ChecklistbankWsClient;
import org.gbif.pipelines.validator.checklists.ws.ChecklistbankWsClientMock;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.junit.jupiter.api.Test;

/** Unit tests related to {@link org.gbif.pipelines.validator.checklists.ChecklistValidator}. */
public class ChecklistValidatorTest {

  @Test
  public void testChecklistValidatorWithoutExtensions() {

    try {
      ChecklistbankWsClient checklistbankWsClient =
          new ChecklistbankWsClientMock("checklists/api_response_without_extensions.json");
      ChecklistValidator checklistValidator = new ChecklistValidator(checklistbankWsClient);

      // When
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              Paths.get(
                  ClassLoader.getSystemResource("checklists/archive_without_extensions.zip")
                      .getFile()));

      // Should
      // Metrics.FileInfo checks
      assertEquals(1, report.size());
      assertEquals(Long.valueOf(20), report.get(0).getCount());
      assertEquals(Long.valueOf(13), report.get(0).getIndexedCount());
      assertEquals(DwcFileType.CORE, report.get(0).getFileType());
      assertEquals(DwcTerm.Taxon.qualifiedName(), report.get(0).getRowType());
      assertEquals("taxon.txt", report.get(0).getFileName());

      // Metrics.IssueInfo checks
      assertEquals(11, report.get(0).getIssues().size());
      assertEquals(Long.valueOf(20), report.get(0).getIssues().get(0).getCount());
      assertTrue(
          report.get(0).getIssues().stream().anyMatch(i -> i.getIssue().equals("missing genus")));
      assertTrue(report.get(0).getIssues().stream().allMatch(i -> i.getSamples().size() == 1));
      assertEquals(
          "gbif.org:species:7471350",
          report.get(0).getIssues().get(0).getSamples().get(0).getRecordId());
      assertEquals(4, report.get(0).getIssues().get(0).getSamples().get(0).getRelatedData().size());
      assertEquals(
          EvaluationCategory.CLB_INTERPRETATION_BASED,
          report.get(0).getIssues().get(0).getIssueCategory());

      assertFalse(report.get(0).getTerms().isEmpty());

      assertTrue(
          report.stream().allMatch(r -> r.getRowType().equals(DwcTerm.Taxon.qualifiedName())));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testChecklistValidatorWithExtensions() {

    try {
      ChecklistbankWsClient checklistbankWsClient =
          new ChecklistbankWsClientMock("checklists/api_response_with_extensions.json");
      ChecklistValidator checklistValidator = new ChecklistValidator(checklistbankWsClient);

      // When
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              Paths.get(
                  ClassLoader.getSystemResource("checklists/archive_with_extensions.zip")
                      .getFile()));

      // Should
      // Metrics.FileInfo checks
      assertEquals(4, report.size());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(DwcTerm.Taxon.qualifiedName()))
              .count());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(GbifTerm.Distribution.qualifiedName()))
              .count());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(DwcTerm.MeasurementOrFact.qualifiedName()))
              .count());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(GbifTerm.Identifier.qualifiedName()))
              .count());

      for (Metrics.FileInfo fileInfo : report) {
        if (fileInfo.getRowType().equals(DwcTerm.Taxon.qualifiedName())) {
          assertEquals(Long.valueOf(7089), fileInfo.getCount());
          assertEquals(Long.valueOf(6673), fileInfo.getIndexedCount());
          assertEquals(DwcFileType.CORE, fileInfo.getFileType());
          assertEquals("taxon.txt", fileInfo.getFileName());

          assertEquals(19, fileInfo.getIssues().size());
          assertEquals(Long.valueOf(7089), fileInfo.getIssues().get(0).getCount());
          assertTrue(
              fileInfo.getIssues().stream()
                  .anyMatch(i -> i.getIssue().equals("distribution area invalid")));
          assertTrue(fileInfo.getIssues().stream().allMatch(i -> i.getSamples().size() == 1));

          assertEquals(
              "gbif.org:species:7471350",
              fileInfo.getIssues().get(0).getSamples().get(0).getRecordId());
          assertEquals(4, fileInfo.getIssues().get(0).getSamples().get(0).getRelatedData().size());
          assertEquals(
              EvaluationCategory.CLB_INTERPRETATION_BASED,
              fileInfo.getIssues().get(0).getIssueCategory());
          assertEquals(15, fileInfo.getTerms().size());
        } else if (fileInfo.getRowType().equals(GbifTerm.Distribution.qualifiedName())) {
          assertEquals(Long.valueOf(7089), fileInfo.getCount());
          assertEquals(Long.valueOf(4444), fileInfo.getIndexedCount());
          assertEquals(DwcFileType.EXTENSION, fileInfo.getFileType());
          assertEquals("distribution.txt", fileInfo.getFileName());
          assertEquals(4, fileInfo.getTerms().size());
          assertTrue(fileInfo.getIssues().isEmpty());
        } else if (fileInfo.getRowType().equals(DwcTerm.MeasurementOrFact.qualifiedName())) {
          assertEquals(Long.valueOf(14178), fileInfo.getCount());
          assertEquals(Long.valueOf(6413), fileInfo.getIndexedCount());
          assertEquals(DwcFileType.EXTENSION, fileInfo.getFileType());
          assertEquals("measurementorfacts.txt", fileInfo.getFileName());
          assertEquals(3, fileInfo.getTerms().size());
          assertTrue(fileInfo.getIssues().isEmpty());
        } else if (fileInfo.getRowType().equals(GbifTerm.Identifier.qualifiedName())) {
          assertEquals(Long.valueOf(7089), fileInfo.getCount());
          assertNull(fileInfo.getIndexedCount());
          assertEquals(DwcFileType.EXTENSION, fileInfo.getFileType());
          assertEquals("identifier.txt", fileInfo.getFileName());
          assertEquals(2, fileInfo.getTerms().size());
          assertTrue(fileInfo.getIssues().isEmpty());
        }
      }

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
