package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class CoreInterpreterTest {

  private static final String ID = "777";

  @Test
  public void interpretSampleSizeValueTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), " value ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    // Should
    Assert.assertNull(br.getSampleSizeValue());
  }

  @Test
  public void interpretSampleSizeUnitTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeUnit.qualifiedName(), " value ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);

    // Should
    Assert.assertEquals("value", br.getSampleSizeUnit());
  }

  @Test
  public void interpretRelativeOrganismQuantityTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), "2");
    coreMap.put(DwcTerm.sampleSizeUnit.qualifiedName(), "some type ");
    coreMap.put(DwcTerm.organismQuantity.qualifiedName(), "10");
    coreMap.put(DwcTerm.organismQuantityType.qualifiedName(), " Some Type");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);
    BasicInterpreter.interpretOrganismQuantity(er, br);
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    BasicInterpreter.interpretRelativeOrganismQuantity(br);

    // Should
    Assert.assertEquals(Double.valueOf(5d), br.getRelativeOrganismQuantity());
  }

  @Test
  public void interpretRelativeOrganismQuantityEmptySamplingTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.organismQuantity.qualifiedName(), "10");
    coreMap.put(DwcTerm.organismQuantityType.qualifiedName(), " Some Type");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);
    BasicInterpreter.interpretOrganismQuantity(er, br);
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    BasicInterpreter.interpretRelativeOrganismQuantity(br);

    // Should
    Assert.assertNull(br.getRelativeOrganismQuantity());
  }

  @Test
  public void interpretRelativeOrganismQuantityEmptyOrganismTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), "2");
    coreMap.put(DwcTerm.sampleSizeUnit.qualifiedName(), "some type ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);
    BasicInterpreter.interpretOrganismQuantity(er, br);
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    BasicInterpreter.interpretRelativeOrganismQuantity(br);

    // Should
    Assert.assertNull(br.getRelativeOrganismQuantity());
  }

  @Test
  public void interpretRelativeOrganismQuantityDiffTypesTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), "2");
    coreMap.put(DwcTerm.sampleSizeUnit.qualifiedName(), "another type ");
    coreMap.put(DwcTerm.organismQuantity.qualifiedName(), "10");
    coreMap.put(DwcTerm.organismQuantityType.qualifiedName(), " Some Type");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);
    BasicInterpreter.interpretOrganismQuantity(er, br);
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    BasicInterpreter.interpretRelativeOrganismQuantity(br);

    // Should
    Assert.assertNull(br.getRelativeOrganismQuantity());
  }

  @Test
  public void interpretRelativeOrganismQuantityNanTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), Double.valueOf("9E99999999").toString());
    coreMap.put(DwcTerm.sampleSizeUnit.qualifiedName(), "another type ");
    coreMap.put(DwcTerm.organismQuantity.qualifiedName(), "10");
    coreMap.put(DwcTerm.organismQuantityType.qualifiedName(), " Some Type");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);
    BasicInterpreter.interpretOrganismQuantity(er, br);
    CoreInterpreter.interpretSampleSizeUnit(er, br::setSampleSizeUnit);
    CoreInterpreter.interpretSampleSizeValue(er, br::setSampleSizeValue);

    BasicInterpreter.interpretRelativeOrganismQuantity(br);

    // Should
    Assert.assertNull(br.getRelativeOrganismQuantity());
  }

  @Test
  public void interpretLicenseTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(
        "http://purl.org/dc/terms/license", "http://creativecommons.org/licenses/by-nc/4.0/");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretLicense(er, br::setLicense);

    // Should
    Assert.assertEquals(License.CC_BY_NC_4_0.name(), br.getLicense());
  }

  @Test
  public void interpretLicenseUnsupportedTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(
        "http://purl.org/dc/terms/license", "http://creativecommons.org/licenses/by-nc/5.0/");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretLicense(er, br::setLicense);

    // Should
    Assert.assertEquals(License.UNSUPPORTED.name(), br.getLicense());
  }

  @Test
  public void interpretLicenseEmptyTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://purl.org/dc/terms/license", "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretLicense(er, br::setLicense);

    // Should
    Assert.assertEquals(License.UNSUPPORTED.name(), br.getLicense());
  }

  @Test
  public void interpretLicenseNullTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://purl.org/dc/terms/license", null);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretLicense(er, br::setLicense);

    // Should
    Assert.assertEquals(License.UNSPECIFIED.name(), br.getLicense());
  }

  @Test
  public void interpretDatasetIDTest() {
    final String id1 = "ID1";
    final String id2 = "ID2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.datasetID.qualifiedName(), id1 + " | " + id2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretDatasetID(er, br::setDatasetID);

    // Should
    Assert.assertEquals(2, br.getDatasetID().size());
    Assert.assertTrue(br.getDatasetID().contains(id1));
    Assert.assertTrue(br.getDatasetID().contains(id2));
    assertIssueSizeZero(br);
  }

  @Test
  public void interpretDatasetNameTest() {
    final String name1 = "name 1";
    final String name2 = "name 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.datasetName.qualifiedName(), name1 + " | " + name2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretDatasetName(er, br::setDatasetName);

    // Should
    Assert.assertEquals(2, br.getDatasetName().size());
    Assert.assertTrue(br.getDatasetName().contains(name1));
    Assert.assertTrue(br.getDatasetName().contains(name2));
    assertIssueSizeZero(br);
  }

  @Test
  public void interpretSamplingProtocolTest() {
    final String sp1 = "protocol";
    final String sp2 = "other protocol";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.samplingProtocol.qualifiedName(), sp1 + " | " + sp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    CoreInterpreter.interpretSamplingProtocol(er, br::setSamplingProtocol);

    // Should
    Assert.assertEquals(2, br.getSamplingProtocol().size());
    Assert.assertTrue(br.getSamplingProtocol().contains(sp1));
    Assert.assertTrue(br.getSamplingProtocol().contains(sp2));
    assertIssueSizeZero(br);
  }

  @Test
  public void interpretLineagesTest() {

    // State
    String eventId = "eventId";
    String parentId = "parentId";
    String parentIdParentId = "parentIdParentId";

    // ExtendedRecord
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.eventID.qualifiedName(), eventId);
    coreMap.put(DwcTerm.parentEventID.qualifiedName(), parentId);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    // EventCoreRecord
    EventCoreRecord evr = EventCoreRecord.newBuilder().setId(eventId).build();

    Map<String, Map<String, String>> source = new HashMap<>();
    source.put(eventId, Collections.singletonMap(DwcTerm.parentEventID.name(), parentId));
    source.put(parentId, Collections.singletonMap(DwcTerm.parentEventID.name(), parentIdParentId));
    source.put(parentIdParentId, Collections.emptyMap());

    // When
    BiConsumer<ExtendedRecord, EventCoreRecord> result =
        CoreInterpreter.interpretLineages(source, null);
    result.accept(er, evr);

    // Should
    Assert.assertEquals(2, evr.getParentsLineage().size());
  }

  @Test
  public void interpretLineagesInfiniteLoopCheckTest() {

    // State
    String eventId = "eventId";

    // ExtendedRecord
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.eventID.qualifiedName(), eventId);
    coreMap.put(DwcTerm.parentEventID.qualifiedName(), eventId);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    // EventCoreRecord
    EventCoreRecord evr = EventCoreRecord.newBuilder().setId(eventId).build();

    Map<String, Map<String, String>> source = new HashMap<>();
    source.put(eventId, Collections.singletonMap(DwcTerm.parentEventID.name(), eventId));

    // When
    BiConsumer<ExtendedRecord, EventCoreRecord> result =
        CoreInterpreter.interpretLineages(source, null);
    result.accept(er, evr);

    // Should
    Assert.assertEquals(0, evr.getParentsLineage().size());
  }

  @Test
  public void interpretLineagesCrossInfiniteLoopCheckTest() {

    // State
    String eventId = "eventId";
    String parentId = "parentId";
    String middleId = "middleId";

    // ExtendedRecord
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.eventID.qualifiedName(), eventId);
    coreMap.put(DwcTerm.parentEventID.qualifiedName(), parentId);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    // EventCoreRecord
    EventCoreRecord evr = EventCoreRecord.newBuilder().setId(eventId).build();

    Map<String, Map<String, String>> source = new HashMap<>();
    source.put(parentId, Collections.singletonMap(DwcTerm.parentEventID.name(), middleId));
    source.put(middleId, Collections.singletonMap(DwcTerm.parentEventID.name(), eventId));
    source.put(eventId, Collections.singletonMap(DwcTerm.parentEventID.name(), parentId));

    // When
    BiConsumer<ExtendedRecord, EventCoreRecord> result =
        CoreInterpreter.interpretLineages(source, null);
    result.accept(er, evr);

    // Should
    Assert.assertEquals(0, evr.getParentsLineage().size());
  }

  private void assertIssueSizeZero(BasicRecord br) {
    assertEquals(0, br.getIssues().getIssueList().size());
  }
}
