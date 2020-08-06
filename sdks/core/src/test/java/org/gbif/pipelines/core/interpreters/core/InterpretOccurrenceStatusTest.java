package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class InterpretOccurrenceStatusTest {

  private static final String ID = "777";

  private static final KeyValueStore<String, OccurrenceStatus> OCCURRENCE_STATUS_VOCABULARY_STUB =
      OccurrenceStatusKvStoreStub.create();

  @Test
  public void When_CountIsNullAndStatusIsNull_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void When_CountIsNullAndStatusIsPresent_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void When_CountIsNullAndStatusIsAbsent_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void When_CountIsNullAndStatusIsRubbish_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
  }

  @Test
  public void When_CountIsGreaterThanZeroAndStatusIsNull_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void When_CountIsGreaterThanZeroAndStatusIsPresent_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void When_CountIsGreaterThanZeroAndStatusIsAbsent_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS, br);
  }

  @Test
  public void When_CountIsGreaterThanZeroAndStatusIsRubbish_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 2);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void When_CountIsZeroAndStatusIsNull_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void When_CountIsZeroAndStatusIsPresent_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS, br);
  }

  @Test
  public void When_CountIsZeroAndStatusIsAbsent_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void When_CountIsZeroAndStatusIsRubbish_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 2);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void When_CountIsRubbishAndStatusIsNull_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, br);
  }

  @Test
  public void When_CountIsRubbishAndStatusIsPresent_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, br);
  }

  @Test
  public void When_CountIsRubbishAndStatusIsAbsent_Expect_Absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 1);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, br);
  }

  @Test
  public void When_CountIsRubbishAndStatusIsRubbish_Expect_Present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    BasicInterpreter.interpretIndividualCount(er, br);

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueSize(br, 2);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
  }

  @NoArgsConstructor(staticName = "create")
  private static class OccurrenceStatusKvStoreStub
      implements KeyValueStore<String, OccurrenceStatus> {

    private static final Map<String, OccurrenceStatus> MAP = new HashMap<>();

    static {
      MAP.put(OccurrenceStatus.ABSENT.name(), OccurrenceStatus.ABSENT);
      MAP.put(OccurrenceStatus.PRESENT.name(), OccurrenceStatus.PRESENT);
    }

    @Override
    public OccurrenceStatus get(String s) {
      return MAP.get(s);
    }

    @Override
    public void close() {}
  }

  private void assertIssueSize(BasicRecord br, int expectedSize) {
    assertEquals(expectedSize, br.getIssues().getIssueList().size());
  }

  private void assertIssueEmpty(BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  private void assertIssue(OccurrenceIssue expectedIssue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(expectedIssue.name()));
  }
}
