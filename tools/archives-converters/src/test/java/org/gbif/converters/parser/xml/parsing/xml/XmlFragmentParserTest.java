package org.gbif.converters.parser.xml.parsing.xml;

import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.converters.parser.xml.identifier.OccurrenceKeyHelper;
import org.gbif.converters.parser.xml.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.converters.parser.xml.identifier.Triplet;
import org.gbif.converters.parser.xml.identifier.UniqueIdentifier;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.io.Resources;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XmlFragmentParserTest {

  @Test
  public void testUtf8a() throws IOException {
    // note the collector name has an u umlaut
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/abcd1_umlaut.xml"), StandardCharsets.UTF_8);

    RawXmlOccurrence rawRecord = createFakeOcc(xml);
    List<RawOccurrenceRecord> results = XmlFragmentParser.parseRecord(rawRecord);
    assertEquals(1, results.size());
    assertEquals("Oschütz", results.get(0).getCollectorName());
  }

  @Ignore("too expensive for constant use")
  @Test
  public void testMultiThreadLoad() throws Exception {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/abcd1_umlaut.xml"), StandardCharsets.UTF_8);

    // 5 parsers in 5 threads parse 1000 records each
    List<RawXmlOccurrence> raws = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      raws.add(createFakeOcc(xml));
    }

    ExecutorService tp = Executors.newFixedThreadPool(5);
    List<Future<List<RawOccurrenceRecord>>> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      int start = i * 1000;
      int end = start + 1000;
      futures.add(tp.submit(new BatchRecordParser(raws.subList(start, end))));
    }

    List<RawOccurrenceRecord> rors = new ArrayList<>();
    for (Future<List<RawOccurrenceRecord>> future : futures) {
      rors.addAll(future.get());
    }

    assertEquals(5000, rors.size());
  }

  private static class BatchRecordParser implements Callable<List<RawOccurrenceRecord>> {

    private final List<RawXmlOccurrence> raws;

    public BatchRecordParser(List<RawXmlOccurrence> raws) {
      this.raws = raws;
    }

    @Override
    public List<RawOccurrenceRecord> call() {
      List<RawOccurrenceRecord> rors = new ArrayList<>();
      for (RawXmlOccurrence raw : raws) {
        rors.addAll(XmlFragmentParser.parseRecord(raw));
      }
      return rors;
    }
  }

  @Test
  public void testIdExtractionSimple() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/abcd1_simple.xml"), StandardCharsets.UTF_8);
    UUID datasetKey = UUID.randomUUID();
    Triplet target =
        new Triplet(
            datasetKey,
            "TLMF",
            "Tiroler Landesmuseum Ferdinandeum",
            "82D45C93-B297-490E-B7B0-E0A9BEED1326",
            null);
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
        XmlFragmentParser.extractIdentifiers(
            datasetKey, xmlBytes, OccurrenceSchemaType.ABCD_1_2, true, true);
    Set<UniqueIdentifier> ids = extractionResults.iterator().next().getUniqueIdentifiers();
    assertEquals(1, ids.size());
    UniqueIdentifier id = ids.iterator().next();
    assertEquals(datasetKey, id.getDatasetKey());
    assertEquals(OccurrenceKeyHelper.buildKey(target), id.getUniqueString());
  }

  @Test
  public void testIdExtractionMultipleIdsUnitQualifier() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/abcd2_multi.xml"), StandardCharsets.UTF_8);

    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
        XmlFragmentParser.extractIdentifiers(
            datasetKey, xmlBytes, OccurrenceSchemaType.ABCD_2_0_6, true, true);
    Triplet triplet1 =
        new Triplet(
            datasetKey, "BGBM", "Bridel Herbar", "Bridel-1-428", "Grimmia alpicola Sw. ex Hedw.");
    Triplet triplet2 =
        new Triplet(
            datasetKey,
            "BGBM",
            "Bridel Herbar",
            "Bridel-1-428",
            "Schistidium agassizii Sull. & Lesq. in Sull.");
    assertEquals(2, extractionResults.size());
    for (IdentifierExtractionResult result : extractionResults) {
      String uniqueId = result.getUniqueIdentifiers().iterator().next().getUniqueString();
      assertTrue(
          uniqueId.equals(OccurrenceKeyHelper.buildKey(triplet1))
              || uniqueId.equals(OccurrenceKeyHelper.buildKey(triplet2)));
    }
  }

  @Test
  public void testIdExtractionWithTripletAndDwcOccurrenceId() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/triplet_and_dwc_id.xml"), StandardCharsets.UTF_8);
    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
        XmlFragmentParser.extractIdentifiers(
            datasetKey, xmlBytes, OccurrenceSchemaType.DWC_1_4, true, true);
    Set<UniqueIdentifier> ids = extractionResults.iterator().next().getUniqueIdentifiers();
    PublisherProvidedUniqueIdentifier pubProvided =
        new PublisherProvidedUniqueIdentifier(datasetKey, "UGENT:vertebrata:50058");
    Triplet triplet = new Triplet(datasetKey, "UGENT", "vertebrata", "50058", null);
    assertEquals(2, ids.size());
    for (UniqueIdentifier id : ids) {
      assertTrue(
          id.getUniqueString().equals(OccurrenceKeyHelper.buildKey(triplet))
              || id.getUniqueString().equals(OccurrenceKeyHelper.buildKey(pubProvided)));
    }
  }

  @Test
  public void testIdExtractTapir() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/tapir_triplet_contains_unrecorded.xml"),
            StandardCharsets.UTF_8);
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
        XmlFragmentParser.extractIdentifiers(
            UUID.randomUUID(), xmlBytes, OccurrenceSchemaType.DWC_1_4, true, true);
    assertFalse(extractionResults.isEmpty());
  }

  @Test
  public void testIdExtractManisBlankCC() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("id_extraction/manis_no_cc.xml"), StandardCharsets.UTF_8);
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
        XmlFragmentParser.extractIdentifiers(
            UUID.randomUUID(), xmlBytes, OccurrenceSchemaType.DWC_MANIS, true, true);
    assertTrue(extractionResults.isEmpty());
  }

  private RawXmlOccurrence createFakeOcc(String xml) {
    RawXmlOccurrence rawRecord = new RawXmlOccurrence();
    rawRecord.setCatalogNumber("fake catalog");
    rawRecord.setCollectionCode("fake collection code");
    rawRecord.setInstitutionCode("fake inst");
    rawRecord.setResourceName("fake resource name");
    rawRecord.setSchemaType(OccurrenceSchemaType.ABCD_1_2);
    rawRecord.setXml(xml);

    return rawRecord;
  }
}
