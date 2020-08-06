package org.gbif.pipelines.core.interpreters.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.License;
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
    coreMap.put(GbifTerm.recordedByID.qualifiedName(), null);
    coreMap.put(GbifTerm.identifiedByID.qualifiedName(), "");
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
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(
        GbifTerm.recordedByID.qualifiedName(),
        " https://orcid.org/0000-0002-0144-1997| https://orcid.org/0000-0002-0144-1997 | https://orcid.org/0000-0002-0144-1997|someid");
    coreMap.put(
        GbifTerm.identifiedByID.qualifiedName(),
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
}
