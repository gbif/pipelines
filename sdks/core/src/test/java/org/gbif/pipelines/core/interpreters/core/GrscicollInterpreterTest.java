package org.gbif.pipelines.core.interpreters.core;

import static org.junit.Assert.assertEquals;

import org.gbif.api.model.collections.lookup.Match.Status;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.junit.Test;

public class GrscicollInterpreterTest {

  @Test
  public void getInstitutionMatchNoneIssuesTest() {
    assertEquals(
        OccurrenceIssue.INSTITUTION_MATCH_NONE,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(null));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_INSTITUTION,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(Status.AMBIGUOUS));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_INSTITUTION,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(Status.AMBIGUOUS_EXPLICIT_MAPPINGS));
    assertEquals(
        OccurrenceIssue.POSSIBLY_ON_LOAN,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(Status.AMBIGUOUS_OWNER));
  }

  @Test
  public void getCollectionMatchNoneIssuesTest() {
    assertEquals(
        OccurrenceIssue.COLLECTION_MATCH_NONE,
        GrscicollInterpreter.getCollectionMatchNoneIssue(null));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_COLLECTION,
        GrscicollInterpreter.getCollectionMatchNoneIssue(Status.AMBIGUOUS));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_COLLECTION,
        GrscicollInterpreter.getCollectionMatchNoneIssue(Status.AMBIGUOUS_EXPLICIT_MAPPINGS));
    assertEquals(
        OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH,
        GrscicollInterpreter.getCollectionMatchNoneIssue(Status.AMBIGUOUS_INSTITUTION_MISMATCH));
  }
}
