package org.gbif.pipelines.tasks.verbatims;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.tasks.verbatims.xml.XmlIdentifierValidation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IdentifierValidationTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static final HdfsConfigs LOCAL = HdfsConfigs.nullConfig();

  @Test
  public void allUniqueTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", "IC", "CC", "1"), record("2", "o2", "IC", "CC", "2"));

    assertEquals(2, report.getCheckedRecords());
    assertEquals(2, report.getUniqueTriplets());
    assertEquals(0, report.getRecordsWithInvalidTriplets());
    assertEquals(2, report.getUniqueOccurrenceIds());
    assertEquals(0, report.getRecordsMissingOccurrenceId());
    assertTrue(XmlIdentifierValidation.tripletsValid(report));
    assertTrue(XmlIdentifierValidation.occurrenceIdsValid(report));
  }

  @Test
  public void duplicateTripletTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", "IC", "CC", "1"), record("2", "o2", "IC", "CC", "1"));

    assertEquals(1, report.getUniqueTriplets());
    assertFalse(XmlIdentifierValidation.tripletsValid(report));
    assertTrue(XmlIdentifierValidation.occurrenceIdsValid(report));
  }

  @Test
  public void incompleteTripletsAreIgnoredTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", "IC", "CC", "1"), record("2", "o2", "IC", null, "2"));

    assertEquals(1, report.getUniqueTriplets());
    assertEquals(1, report.getRecordsWithInvalidTriplets());
    assertTrue(XmlIdentifierValidation.tripletsValid(report));
  }

  @Test
  public void noTripletsTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", null, null, null), record("2", "o2", "", "", ""));

    assertEquals(0, report.getUniqueTriplets());
    assertEquals(2, report.getRecordsWithInvalidTriplets());
    assertFalse(XmlIdentifierValidation.tripletsValid(report));
    assertTrue(XmlIdentifierValidation.occurrenceIdsValid(report));
  }

  @Test
  public void duplicateOccurrenceIdTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", "IC", "CC", "1"), record("2", "o1", "IC", "CC", "2"));

    assertEquals(1, report.getUniqueOccurrenceIds());
    assertTrue(XmlIdentifierValidation.tripletsValid(report));
    assertFalse(XmlIdentifierValidation.occurrenceIdsValid(report));
  }

  @Test
  public void missingOccurrenceIdTest() {
    OccurrenceValidationReport report =
        validate(record("1", "o1", "IC", "CC", "1"), record("2", " ", "IC", "CC", "2"));

    assertEquals(1, report.getRecordsMissingOccurrenceId());
    assertFalse(XmlIdentifierValidation.occurrenceIdsValid(report));
  }

  @Test
  public void noReportIsValidTest() {
    assertTrue(XmlIdentifierValidation.tripletsValid(null));
    assertTrue(XmlIdentifierValidation.occurrenceIdsValid(null));
  }

  @Test
  public void avroAndMetaFileRoundTripTest() throws Exception {
    File avro = tmp.newFile("verbatim.avro");
    writeAvro(
        avro,
        Arrays.asList(
            record("1", "o1", "IC", "CC", "1"),
            record("2", "o1", "IC", "CC", "2"),
            record("3", null, "IC", "CC", null)));

    OccurrenceValidationReport report =
        XmlIdentifierValidation.validate(LOCAL, avro.getAbsolutePath());
    assertEquals(3, report.getCheckedRecords());
    assertEquals(2, report.getUniqueTriplets());
    assertEquals(1, report.getRecordsWithInvalidTriplets());
    assertEquals(1, report.getUniqueOccurrenceIds());
    assertEquals(1, report.getRecordsMissingOccurrenceId());

    // existing metrics are kept, the report is appended
    File meta = tmp.newFile("archive-to-verbatim.yml");
    // as written by ConverterToVerbatim, UTF-16 via writeChars
    Files.write(meta.toPath(), "archiveToOccurrenceCount: 3".getBytes(StandardCharsets.UTF_16BE));
    XmlIdentifierValidation.appendToMetaFile(LOCAL, meta.getAbsolutePath(), report);

    // written with writeChars like the other meta files
    String body =
        new String(Files.readAllBytes(meta.toPath()), StandardCharsets.UTF_8).replace("\u0000", "");
    assertTrue(body, body.startsWith("archiveToOccurrenceCount: 3\n"));
    assertTrue(body, body.contains(XmlIdentifierValidation.CHECKED_RECORDS + ": 3\n"));

    Optional<OccurrenceValidationReport> read =
        XmlIdentifierValidation.readFromMetaFile(LOCAL, meta.getAbsolutePath());
    assertTrue(read.isPresent());
    assertEquals(report, read.get());
  }

  @Test
  public void metaFileWithoutReportTest() throws Exception {
    File meta = tmp.newFile("archive-to-verbatim.yml");
    Files.write(meta.toPath(), "archiveToOccurrenceCount: 3\n".getBytes(StandardCharsets.UTF_8));
    assertFalse(
        XmlIdentifierValidation.readFromMetaFile(LOCAL, meta.getAbsolutePath()).isPresent());
  }

  private static OccurrenceValidationReport validate(ExtendedRecord... records) {
    return XmlIdentifierValidation.validate(Arrays.asList(records).iterator());
  }

  private static ExtendedRecord record(
      String id, String occurrenceId, String ic, String cc, String cn) {
    Map<String, String> terms = new HashMap<>();
    put(terms, DwcTerm.occurrenceID, occurrenceId);
    put(terms, DwcTerm.institutionCode, ic);
    put(terms, DwcTerm.collectionCode, cc);
    put(terms, DwcTerm.catalogNumber, cn);
    return ExtendedRecord.newBuilder()
        .setId(id)
        .setCoreTerms(terms)
        .setExtensions(Collections.emptyMap())
        .build();
  }

  private static void put(Map<String, String> terms, DwcTerm term, String value) {
    if (value != null) {
      terms.put(term.qualifiedName(), value);
    }
  }

  private static void writeAvro(File file, List<ExtendedRecord> records) throws Exception {
    try (DataFileWriter<ExtendedRecord> writer =
        new DataFileWriter<>(new SpecificDatumWriter<>(ExtendedRecord.class))) {
      writer.create(ExtendedRecord.getClassSchema(), new FileOutputStream(file));
      for (ExtendedRecord r : records) {
        writer.append(r);
      }
    }
  }
}
