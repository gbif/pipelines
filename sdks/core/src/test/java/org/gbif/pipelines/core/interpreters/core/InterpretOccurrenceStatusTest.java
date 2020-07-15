package org.gbif.pipelines.core.interpreters.core;

import java.util.HashMap;
import java.util.Map;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.junit.Test;

import lombok.NoArgsConstructor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InterpretOccurrenceStatusTest {

  private static final String ID = "777";

  private static final KeyValueStore<String, OccurrenceStatus> OCCURRENCE_STATUS_VOCABULARY_STUB =
      OccurrenceStatusKvStoreStub.create();

  @Test
  public void when_countIsNullAndStatusIsNull_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void when_countIsNullAndStatusIsPresent_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void when_countIsNullAndStatusIsAbsent_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void when_countIsNullAndStatusIsRubbish_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), null);
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
  }

  @Test
  public void when_countIsGreaterThanZeroAndStatusIsNull_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void when_countIsGreaterThanZeroAndStatusIsPresent_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void when_countIsGreaterThanZeroAndStatusIsAbsent_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS, br);
  }

  @Test
  public void when_countIsGreaterThanZeroAndStatusIsRubbish_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "1");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void when_countIsZeroAndStatusIsNull_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void when_countIsZeroAndStatusIsPresent_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void when_countIsZeroAndStatusIsAbsent_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssueEmpty(br);
  }

  @Test
  public void when_countIsZeroAndStatusIsRubbish_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "0");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE, br);
    assertIssue(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT, br);
  }

  @Test
  public void when_countIsRubbishAndStatusIsNull_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), null);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_UNPARSABLE, br);
  }

  @Test
  public void when_countIsRubbishAndStatusIsPresent_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_UNPARSABLE, br);
  }

  @Test
  public void when_countIsRubbishAndStatusIsAbsent_expect_absent() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_UNPARSABLE, br);
  }

  @Test
  public void when_countIsRubbishAndStatusIsRubbish_expect_present() {
    // State
    Map<String, String> coreTerms = new HashMap<>(2);
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "blabla");
    coreTerms.put(DwcTerm.occurrenceStatus.qualifiedName(), "blabla");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreTerms).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertIssue(OccurrenceIssue.INDIVIDUAL_COUNT_UNPARSABLE, br);
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

  private void assertIssueEmpty(BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  private void assertIssue(OccurrenceIssue issue, BasicRecord br) {
    assertTrue(br.getIssues().getIssueList().contains(issue.name()));
  }
}
