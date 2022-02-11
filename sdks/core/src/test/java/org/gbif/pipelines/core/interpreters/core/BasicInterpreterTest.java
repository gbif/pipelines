package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class BasicInterpreterTest {

  private static final String ID = "777";

  @Test
  public void interpretIndividaulCountTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.individualCount.qualifiedName(), "2");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIndividualCount(er, br);

    // Should
    Assert.assertEquals(Integer.valueOf(2), br.getIndividualCount());
  }

  @Test
  public void interpretIndividaulCountNegativedTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.individualCount.qualifiedName(), "-2");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIndividualCount(er, br);

    // Should
    Assert.assertNull(br.getIndividualCount());
    Assert.assertTrue(
        br.getIssues().getIssueList().contains(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID.name()));
  }

  @Test
  public void interpretIndividaulCountInvalidTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.individualCount.qualifiedName(), "2.666666667");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIndividualCount(er, br);

    // Should
    Assert.assertNull(br.getIndividualCount());
    Assert.assertTrue(
        br.getIssues().getIssueList().contains(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID.name()));
  }

  @Test
  public void interpretSampleSizeValueTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.sampleSizeValue.qualifiedName(), " value ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretSampleSizeUnit(er, br);

    // Should
    Assert.assertEquals("value", br.getSampleSizeUnit());
  }

  @Test
  public void interpretOrganismQuantityTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.organismQuantity.qualifiedName(), " value ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantity(er, br);

    // Should
    Assert.assertNull(br.getOrganismQuantity());
  }

  @Test
  public void interpretOrganismQuantityTypeTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.organismQuantityType.qualifiedName(), " value ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOrganismQuantityType(er, br);

    // Should
    Assert.assertEquals("value", br.getOrganismQuantityType());
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
    BasicInterpreter.interpretSampleSizeUnit(er, br);
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretSampleSizeUnit(er, br);
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretSampleSizeUnit(er, br);
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretSampleSizeUnit(er, br);
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretSampleSizeUnit(er, br);
    BasicInterpreter.interpretSampleSizeValue(er, br);

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
    BasicInterpreter.interpretLicense(er, br);

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
    BasicInterpreter.interpretLicense(er, br);

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
    BasicInterpreter.interpretLicense(er, br);

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
    BasicInterpreter.interpretLicense(er, br);

    // Should
    Assert.assertEquals(License.UNSPECIFIED.name(), br.getLicense());
  }

  @Test
  public void interpretAgentIdsEmtyOrNullTest() {

    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.recordedByID.qualifiedName(), null);
    coreMap.put(DwcTerm.identifiedByID.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretRecordedByIds(er, br);
    BasicInterpreter.interpretIdentifiedByIds(er, br);

    // Should
    Assert.assertTrue(br.getRecordedByIds().isEmpty());
    Assert.assertTrue(br.getIdentifiedByIds().isEmpty());
  }

  @Test
  public void interpretAgentIdsTest() {

    // Expected
    List<AgentIdentifier> expectedRecorded =
        Stream.of(
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.ORCID.name())
                    .setValue("https://orcid.org/0000-0002-0144-1997")
                    .build(),
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.OTHER.name())
                    .setValue("someid")
                    .build())
            .sorted()
            .collect(Collectors.toList());

    List<AgentIdentifier> expectedIdentified =
        Stream.of(
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.ORCID.name())
                    .setValue("https://orcid.org/0000-0002-0144-1997")
                    .build(),
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.WIKIDATA.name())
                    .setValue("http://www.wikidata.org/entity/1997")
                    .build(),
                AgentIdentifier.newBuilder()
                    .setType(AgentIdentifierType.OTHER.name())
                    .setValue("http://www.somelink.org/id/idid")
                    .build())
            .sorted()
            .collect(Collectors.toList());

    // State
    Map<String, String> coreMap = new HashMap<>(2);
    coreMap.put(
        DwcTerm.recordedByID.qualifiedName(),
        " https://orcid.org/0000-0002-0144-1997| https://orcid.org/0000-0002-0144-1997 | https://orcid.org/0000-0002-0144-1997|someid");
    coreMap.put(
        DwcTerm.identifiedByID.qualifiedName(),
        " https://orcid.org/0000-0002-0144-1997|https://orcid.org/0000-0002-0144-1997 |http://www.wikidata.org/entity/1997|http://www.somelink.org/id/idid");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIdentifiedByIds(er, br);
    BasicInterpreter.interpretRecordedByIds(er, br);

    // Should
    Assert.assertEquals(
        expectedIdentified, br.getIdentifiedByIds().stream().sorted().collect(Collectors.toList()));
    Assert.assertEquals(
        expectedRecorded, br.getRecordedByIds().stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void interpretBasisOfRecordTest() {

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), "LIVING_SPECIMEN");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    Assert.assertEquals("LIVING_SPECIMEN", br.getBasisOfRecord());
  }

  @Test
  public void interpretBasisOfRecordNullTest() {

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), null);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    Assert.assertEquals("OCCURRENCE", br.getBasisOfRecord());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.BASIS_OF_RECORD_INVALID, br);
  }

  @Test
  public void interpretBasisOfRecordRubbishTest() {

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), "adwadaw");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    Assert.assertEquals("OCCURRENCE", br.getBasisOfRecord());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.BASIS_OF_RECORD_INVALID, br);
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
    BasicInterpreter.interpretDatasetID(er, br);

    // Should
    Assert.assertEquals(2, br.getDatasetID().size());
    Assert.assertTrue(br.getDatasetID().contains(id1));
    Assert.assertTrue(br.getDatasetID().contains(id2));
    assertIssueSize(br, 0);
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
    BasicInterpreter.interpretDatasetName(er, br);

    // Should
    Assert.assertEquals(2, br.getDatasetName().size());
    Assert.assertTrue(br.getDatasetName().contains(name1));
    Assert.assertTrue(br.getDatasetName().contains(name2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretOtherCatalogNumbersTest() {
    final String number1 = "111";
    final String number2 = "22";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.otherCatalogNumbers.qualifiedName(), number1 + " | " + number2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOtherCatalogNumbers(er, br);

    // Should
    Assert.assertEquals(2, br.getOtherCatalogNumbers().size());
    Assert.assertTrue(br.getOtherCatalogNumbers().contains(number1));
    Assert.assertTrue(br.getOtherCatalogNumbers().contains(number2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretRecordedByTest() {
    final String person1 = "person 1";
    final String person2 = "person, 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.recordedBy.qualifiedName(), person1 + " | " + person2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretRecordedBy(er, br);

    // Should
    Assert.assertEquals(2, br.getRecordedBy().size());
    Assert.assertTrue(br.getRecordedBy().contains(person1));
    Assert.assertTrue(br.getRecordedBy().contains(person2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretIdentifiedByTest() {
    final String person1 = "person 1";
    final String person2 = "person, 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.identifiedBy.qualifiedName(), person1 + " | " + person2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIdentifiedBy(er, br);

    // Should
    Assert.assertEquals(2, br.getIdentifiedBy().size());
    Assert.assertTrue(br.getIdentifiedBy().contains(person1));
    Assert.assertTrue(br.getIdentifiedBy().contains(person2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretPreparationsTest() {
    final String prep1 = "prep 1";
    final String prep2 = "prep 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.preparations.qualifiedName(), prep1 + " | " + prep2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretPreparations(er, br);

    // Should
    Assert.assertEquals(2, br.getPreparations().size());
    Assert.assertTrue(br.getPreparations().contains(prep1));
    Assert.assertTrue(br.getPreparations().contains(prep2));
    assertIssueSize(br, 0);
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
    BasicInterpreter.interpretSamplingProtocol(er, br);

    // Should
    Assert.assertEquals(2, br.getSamplingProtocol().size());
    Assert.assertTrue(br.getSamplingProtocol().contains(sp1));
    Assert.assertTrue(br.getSamplingProtocol().contains(sp2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusTest() {
    final String tp1 = TypeStatus.TYPE.name();
    final String tp2 = TypeStatus.ALLOTYPE.name();

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretTypeStatus(er, br);

    // Should
    Assert.assertEquals(2, br.getTypeStatus().size());
    Assert.assertTrue(br.getTypeStatus().contains(tp1));
    Assert.assertTrue(br.getTypeStatus().contains(tp2));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretTypeStatusPartiallyInvalidTest() {
    final String tp1 = TypeStatus.TYPE.name();
    final String tp2 = "foo";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.typeStatus.qualifiedName(), tp1 + " | " + tp2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretTypeStatus(er, br);

    // Should
    Assert.assertEquals(1, br.getTypeStatus().size());
    Assert.assertEquals(tp1, br.getTypeStatus().get(0));
    assertIssueSize(br, 1);
  }

  private void assertIssueSize(BasicRecord br, int expectedSize) {
    assertEquals(expectedSize, br.getIssues().getIssueList().size());
  }

  private void assertIssue(OccurrenceIssue expectedIssue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(expectedIssue.name()));
  }
}
