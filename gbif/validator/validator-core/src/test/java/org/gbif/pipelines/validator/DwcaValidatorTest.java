package org.gbif.pipelines.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.junit.Ignore;
import org.junit.Test;

public class DwcaValidatorTest {

  @Test
  @Ignore("manual test to validate archives")
  public void manualUrlTest() throws IOException {
    URI dwca = URI.create("http://pensoft.net/dwc/bdj/checklist_980.zip");

    File tmp = File.createTempFile("gbif", "dwca");
    tmp.deleteOnExit();
    File dwcaDir = org.gbif.utils.file.FileUtils.createTempDir();
    dwcaDir.deleteOnExit();

    FileUtils.copyURLToFile(dwca.toURL(), tmp);

    Archive archive = DwcFiles.fromCompressed(tmp.toPath(), dwcaDir.toPath());

    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    System.out.println(report);
  }

  @Test
  public void goodTripletsGoodIdsTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-good-ids.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());
  }

  @Test
  public void checklistGoodTripletsGoodIdsTest() throws IOException {

    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca_checklist-one-hundred-good-triplets-good-ids.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.CHECKLIST)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(100, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());
  }

  @Test
  public void emlOnlyTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/eml.xml");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.METADATA)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertTrue(report.isValid());
  }

  @Test
  public void goodTripletsNoOccurrenceIdTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-thousand-good-triplets-no-id.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(1000, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(1000, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(1000, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertEquals(
        report.getOccurrenceReport().getUniqueTriplets(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(
        report.getOccurrenceReport().getUniqueOccurrenceIds(),
        report.getOccurrenceReport().getCheckedRecords()
            - report.getOccurrenceReport().getRecordsMissingOccurrenceId());
  }

  @Test
  public void dupeTripletTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-dupe-triplet.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(10, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
  }

  @Test
  public void invalidTripletInValidArchiveTest() throws IOException {
    // State
    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-20-percent-invalid-triplet.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(80, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(20, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());
  }

  @Test
  public void goodTripletsDupedIdsTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-dupe-ids.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(90, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());
  }

  @Test
  public void goodTripletsDupedAndMissingIdsTest() throws IOException {
    // State
    Archive archive =
        DwcaTestUtil.openArchive("/dwca/dwca-one-hundred-good-triplets-dupe-and-missing-ids.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(100, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(80, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(10, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertTrue(report.getOccurrenceReport().isAllRecordsChecked());
    assertNull(report.getInvalidationReason());
  }

  @Test
  public void invalidAndDupeTripletTest() throws IOException {
    // State
    Archive archive =
        DwcaTestUtil.openArchive(
            "/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(5, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(50, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
        "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
        report.getOccurrenceReport().getInvalidationReason());
  }

  @Test
  public void dupeAndBadTripletNoOccurrenceIdTest() throws IOException {
    // State
    Archive archive =
        DwcaTestUtil.openArchive(
            "/dwca/dwca-one-hundred-50-percent-invalid-with-dupes-triplet.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(100, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(5, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(50, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(100, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertFalse(report.isValid());
    assertEquals(
        "Archive invalid because [50% invalid triplets is > than threshold of 25%; 45 duplicate triplets detected; 100 records without an occurrence id (should be 0)]",
        report.getOccurrenceReport().getInvalidationReason());
  }

  @Test
  public void emptyArchiveTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/dwca-empty.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.OCCURRENCE)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertEquals(0, report.getOccurrenceReport().getCheckedRecords());
    assertEquals(0, report.getOccurrenceReport().getUniqueTriplets());
    assertEquals(0, report.getOccurrenceReport().getRecordsWithInvalidTriplets());
    assertEquals(0, report.getOccurrenceReport().getUniqueOccurrenceIds());
    assertEquals(0, report.getOccurrenceReport().getRecordsMissingOccurrenceId());
    assertTrue(report.isValid());
    assertNull(report.getOccurrenceReport().getInvalidationReason());
  }

  @Test
  public void goodChecklistTaxonIDTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_good_taxonid.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.CHECKLIST)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertTrue(report.getGenericReport().getDuplicateIds().isEmpty());
    assertTrue(report.getGenericReport().getRowNumbersMissingId().isEmpty());
  }

  @Test
  public void goodGenericCoreTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_good_coreid.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.CHECKLIST)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertTrue("Validation failed: " + report.getInvalidationReason(), report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(0, report.getGenericReport().getDuplicateIds().size());
    assertEquals(0, report.getGenericReport().getRowNumbersMissingId().size());
  }

  @Test
  public void badGenericMissingTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_missing_taxonid.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.CHECKLIST)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(0, report.getGenericReport().getDuplicateIds().size());
    assertEquals(2, report.getGenericReport().getRowNumbersMissingId().size());
  }

  @Test
  public void badGenericDuplTest() throws IOException {
    // State
    Archive archive = DwcaTestUtil.openArchive("/dwca/checklist_dupl_coreid.zip");

    // When
    DwcaValidationReport report =
        DwcaValidator.builder()
            .datasetKey(UUID.randomUUID())
            .datasetType(DatasetType.CHECKLIST)
            .archive(archive)
            .build()
            .validate();

    // Should
    assertFalse("Validation succeeded", report.isValid());
    assertEquals(15, report.getGenericReport().getCheckedRecords());
    assertEquals(1, report.getGenericReport().getDuplicateIds().size());
    assertEquals(0, report.getGenericReport().getRowNumbersMissingId().size());
  }
}
