package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.Extension.DNA_DERIVED_DATA;
import static org.gbif.api.vocabulary.Extension.GEL_IMAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
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
  public void interpretSemicolonOtherCatalogNumbersTest() {
    final String number1 = "111";
    final String number2 = "22";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.otherCatalogNumbers.qualifiedName(), number1 + " ; " + number2 + " ; ");
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
  public void interpretIdentifiedByFromIdentificationExtensionTest() {
    final String person1 = "person 1";
    final String person2 = "person, 2";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.identifiedBy.qualifiedName(), person1 + " | " + person2 + " | ");
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setExtensions(ext).build();

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
  public void interpretIdentifiedByCorePreferenceOverExtensionTest() {
    final String person1 = "person 1";
    final String person2 = "person, 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.identifiedBy.qualifiedName(), person1);
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.identifiedBy.qualifiedName(), person2);
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).setExtensions(ext).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIdentifiedBy(er, br);

    // Should
    Assert.assertEquals(1, br.getIdentifiedBy().size());
    Assert.assertEquals(person1, br.getIdentifiedBy().get(0));
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretIdentifiedByIgnoreIdentificationExtensionTest() {
    final String person1 = "person 1";
    final String person2 = "person, 2";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    // we set another identification term and the extension should be ignored
    coreMap.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.identifiedBy.qualifiedName(), person2);
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).setExtensions(ext).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIdentifiedBy(er, br);

    // Should
    Assert.assertEquals(0, br.getIdentifiedBy().size());
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

  @Test
  public void interpretIsSequencedNullTest() {
    final String seq = "null";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.associatedSequences.qualifiedName(), seq);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er, br);

    // Should
    Assert.assertFalse(br.getIsSequenced());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretIsSequencedAssociatedSequencesTest() {

    // State
    final String seq = " awdawd ";
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.associatedSequences.qualifiedName(), seq);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er, br);

    // Should
    Assert.assertTrue(br.getIsSequenced());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretIsSequencedExtensionTest() {

    // State
    Map<String, List<Map<String, String>>> extension =
        Collections.singletonMap(
            DNA_DERIVED_DATA.getRowType(),
            Collections.singletonList(Collections.singletonMap("awd", "daw")));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setExtensions(extension).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er, br);

    // Should
    Assert.assertTrue(br.getIsSequenced());
    assertIssueSize(br, 0);

    // State
    Map<String, List<Map<String, String>>> extension2 =
        Collections.singletonMap(
            GEL_IMAGE.getRowType(),
            Collections.singletonList(Collections.singletonMap("awd", "daw")));
    ExtendedRecord er2 = ExtendedRecord.newBuilder().setId(ID).setExtensions(extension2).build();
    BasicRecord br2 = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er2, br2);

    // Should
    Assert.assertTrue(br2.getIsSequenced());
    assertIssueSize(br2, 0);
  }

  @Test
  public void interpretIsSequencedExtensionEmptyTest() {

    // State
    Map<String, List<Map<String, String>>> extension =
        Collections.singletonMap(DNA_DERIVED_DATA.getRowType(), Collections.emptyList());
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setExtensions(extension).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er, br);

    // Should
    Assert.assertFalse(br.getIsSequenced());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretIsSequencedMixedTest() {

    // State
    final String seq = " awdawd ";
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.associatedSequences.qualifiedName(), seq);
    Map<String, List<Map<String, String>>> extension =
        Collections.singletonMap(DNA_DERIVED_DATA.getRowType(), Collections.emptyList());

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(coreMap)
            .setExtensions(extension)
            .build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretIsSequenced(er, br);

    // Should
    Assert.assertTrue(br.getIsSequenced());
    assertIssueSize(br, 0);
  }

  @Test
  public void interpretAssociatedSequencesTest() {
    final String number1 = "111";
    final String number2 = "22";
    final String number3 = "33";

    // State
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(
        DwcTerm.associatedSequences.qualifiedName(), number1 + " | " + number2 + " ; " + number3);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretAssociatedSequences(er, br);

    // Should
    Assert.assertEquals(3, br.getAssociatedSequences().size());
    Assert.assertTrue(br.getAssociatedSequences().contains(number1));
    Assert.assertTrue(br.getAssociatedSequences().contains(number2));
    Assert.assertTrue(br.getAssociatedSequences().contains(number3));
    assertIssueSize(br, 0);
  }

  private void assertIssueSize(BasicRecord br, int expectedSize) {
    assertEquals(expectedSize, br.getIssues().getIssueList().size());
  }

  private void assertIssue(OccurrenceIssue expectedIssue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(expectedIssue.name()));
  }
}
