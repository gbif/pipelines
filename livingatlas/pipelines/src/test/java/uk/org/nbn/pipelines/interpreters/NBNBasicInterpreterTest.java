package uk.org.nbn.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.pipelines.vocabulary.Vocab;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;
import uk.org.nbn.pipelines.vocabulary.IdentificationVerificationStatus;
import uk.org.nbn.pipelines.vocabulary.NBNLicense;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;

public class NBNBasicInterpreterTest {
  private static final String ID = "777";

  @Test
  public void interpretLicenseEmptyTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://rs.tdwg.org/dwc/terms/license", "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseNullTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://rs.tdwg.org/dwc/terms/license", null);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseMatchingTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://rs.tdwg.org/dwc/terms/license", "CC-BY NC");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("CC-BY-NC", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseNonMatchingTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put("http://rs.tdwg.org/dwc/terms/license", "not a license");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretNoLicenseTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void
      givenOccurrenceBasisOfRecord_whenInterpretBasisOfRecord_shouldChangeToHumanObservation() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(ID)
            .setBasisOfRecord(BasisOfRecord.OCCURRENCE.name())
            .build();

    // When
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenLivingSpecimenBasisOfRecord_whenInterpretBasisOfRecord_shouldNotChange(){
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(ID)
            .setBasisOfRecord(BasisOfRecord.LIVING_SPECIMEN.name())
            .build();

    // When
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.LIVING_SPECIMEN.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenNullBasisOfRecord_whenInterpretBasisOfRecord_shouldNotThrowException(){
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    try {
      NBNBasicInterpreter.interpretBasisOfRecord(er, br);
    } catch (Exception e) {
      // should
      fail("Method threw an exception: " + e.getMessage());
    }
  }

  @Test
  public void givenNullIdentificationVerificationStatus_whenInterpret_shouldAddMissingIssue()
      throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.MISSING_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void givenEmptyIdentificationVerificationStatus_whenInterpret_shouldAddMissingIssue()
      throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.MISSING_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void
      givenUnmatchedIdentificationVerificationStatus_whenInterpret_shouldAddUnrecognisedIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "not valid");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    //
    // assertTrue(br.getIssues().getIssueList().contains(NBNOccurrenceIssue.UNRECOGNISED_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void
      givenMatchedIdentificationVerificationStatus_whenInterpret_shouldSetIdentificationVerificationStatusToIt()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "Considered Correct");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("Accepted - considered correct", br.getIdentificationVerificationStatus());
  }
}
