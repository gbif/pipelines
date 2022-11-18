package org.gbif.pipelines.validator;

import static org.junit.Assert.assertEquals;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.validator.api.EvaluationType;
import org.junit.Test;

public class EvaluationTypeTest {

  @Test
  public void addNewEvaluationTypeTest() {
    // When we add new OccurrenceIssue this will fail, then we need add new value to EvaluationType
    assertEquals(69, OccurrenceIssue.values().length);
    assertEquals(104, EvaluationType.values().length);
  }
}
