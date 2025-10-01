package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Status.*;
import static org.junit.Assert.assertEquals;

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
        GrscicollInterpreter.getInstitutionMatchNoneIssue(AMBIGUOUS));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_INSTITUTION,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(AMBIGUOUS_EXPLICIT_MAPPINGS));
    assertEquals(
        OccurrenceIssue.DIFFERENT_OWNER_INSTITUTION,
        GrscicollInterpreter.getInstitutionMatchNoneIssue(AMBIGUOUS_OWNER));
  }

  @Test
  public void getCollectionMatchNoneIssuesTest() {
    assertEquals(
        OccurrenceIssue.COLLECTION_MATCH_NONE,
        GrscicollInterpreter.getCollectionMatchNoneIssue(null));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_COLLECTION,
        GrscicollInterpreter.getCollectionMatchNoneIssue(AMBIGUOUS));
    assertEquals(
        OccurrenceIssue.AMBIGUOUS_COLLECTION,
        GrscicollInterpreter.getCollectionMatchNoneIssue(AMBIGUOUS_EXPLICIT_MAPPINGS));
    assertEquals(
        OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH,
        GrscicollInterpreter.getCollectionMatchNoneIssue(AMBIGUOUS_INSTITUTION_MISMATCH));
  }
}
