package org.gbif.pipelines.validator.checklist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import life.catalogue.coldp.ColdpTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.junit.jupiter.api.Test;

/** Unit tests related to {@link ChecklistValidator}. */
public class ChecklistValidatorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String REGISTRY_API_URL = "https://api.gbif-dev.org";

  @Test
  public void testChecklistValidatorWithoutExtensions() {

    try {
      ChecklistbankWsClient checklistbankWsClient = new ChecklistbankWsClientMock();
      ChecklistValidator checklistValidator =
          new ChecklistValidator(checklistbankWsClient, REGISTRY_API_URL, null);

      // it simulates the response received in the callback
      ClbDatasetImport clbDatasetImport =
          OBJECT_MAPPER.readValue(
              ClassLoader.getSystemResourceAsStream(
                  "checklists/api_response_without_extensions.json"),
              ClbDatasetImport.class);

      // When
      List<Metrics.FileInfo> report = checklistValidator.evaluateResults(clbDatasetImport);

      // Should
      // Metrics.FileInfo checks
      assertEquals(1, report.size());
      assertEquals(Long.valueOf(20), report.get(0).getCount());
      assertEquals(Long.valueOf(13), report.get(0).getIndexedCount());
      assertEquals(5, report.get(0).getTerms().size());
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
      ChecklistbankWsClient checklistbankWsClient = new ChecklistbankWsClientMock();
      ChecklistValidator checklistValidator =
          new ChecklistValidator(checklistbankWsClient, REGISTRY_API_URL, null);

      // it simulates the response received in the callback
      ClbDatasetImport clbDatasetImport =
          OBJECT_MAPPER.readValue(
              ClassLoader.getSystemResourceAsStream("checklists/api_response_with_extensions.json"),
              ClbDatasetImport.class);

      // When
      List<Metrics.FileInfo> report = checklistValidator.evaluateResults(clbDatasetImport);

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

  @Test
  public void testChecklistValidatorColDP() {

    try {
      ChecklistbankWsClient checklistbankWsClient =
          new ChecklistbankWsClientMock("checklists/api_response_verbatim_coldp.json");
      ChecklistValidator checklistValidator =
          new ChecklistValidator(checklistbankWsClient, REGISTRY_API_URL, null);

      // it simulates the response received in the callback
      ClbDatasetImport clbDatasetImport =
          OBJECT_MAPPER.readValue(
              ClassLoader.getSystemResourceAsStream("checklists/api_response_coldp.json"),
              ClbDatasetImport.class);

      // When
      List<Metrics.FileInfo> report = checklistValidator.evaluateResults(clbDatasetImport);

      // Should
      // Metrics.FileInfo checks
      assertEquals(2, report.size());

      assertEquals(2, report.size());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(ColdpTerm.NameUsage.qualifiedName()))
              .count());
      assertEquals(
          1,
          report.stream()
              .filter(r -> r.getRowType().equals(ColdpTerm.VernacularName.qualifiedName()))
              .count());

      for (Metrics.FileInfo fileInfo : report) {
        assertNull(fileInfo.getFileType());

        if (fileInfo.getRowType().equals(ColdpTerm.NameUsage.qualifiedName())) {
          assertEquals(Long.valueOf(93982), fileInfo.getCount());
          assertEquals(Long.valueOf(93966), fileInfo.getIndexedCount());
          assertEquals(8, fileInfo.getTerms().size());

          assertEquals("col:NameUsage.txt", fileInfo.getFileName());

          assertEquals(15, fileInfo.getIssues().size());
          assertEquals(Long.valueOf(33822), fileInfo.getIssues().get(0).getCount());
          assertTrue(
              fileInfo.getIssues().stream()
                  .anyMatch(i -> i.getIssue().equals("missing authorship")));
          assertTrue(fileInfo.getIssues().stream().allMatch(i -> i.getSamples().size() == 1));

          assertEquals("DICH10", fileInfo.getIssues().get(0).getSamples().get(0).getRecordId());
          assertEquals(5, fileInfo.getIssues().get(0).getSamples().get(0).getRelatedData().size());
          assertEquals(
              EvaluationCategory.CLB_INTERPRETATION_BASED,
              fileInfo.getIssues().get(0).getIssueCategory());
        } else if (fileInfo.getRowType().equals(ColdpTerm.VernacularName.qualifiedName())) {
          assertEquals(Long.valueOf(43776), fileInfo.getCount());
          assertEquals(Long.valueOf(43776), fileInfo.getIndexedCount());
          assertEquals("col:VernacularName.txt", fileInfo.getFileName());
          assertEquals(3, fileInfo.getTerms().size());
          assertTrue(fileInfo.getIssues().isEmpty());
        }
      }

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
