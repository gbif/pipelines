package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
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

  @Test
  public void interpretProjectIdTest() {
    final String id1 = "projectId1";
    final String id2 = "projectId2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(GbifTerm.projectId.qualifiedName(), id1 + " | " + id2 + " | ");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretProjectId(er, br);

    // Should
    Assert.assertEquals(2, br.getProjectId().size());
    Assert.assertTrue(br.getProjectId().contains(id1));
    Assert.assertTrue(br.getProjectId().contains(id2));
    assertIssueSize(br, 0);
  }

  private void assertIssueSize(BasicRecord br, int expectedSize) {
    assertEquals(expectedSize, br.getIssues().getIssueList().size());
  }

  private void assertIssue(OccurrenceIssue expectedIssue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(expectedIssue.name()));
  }
}
