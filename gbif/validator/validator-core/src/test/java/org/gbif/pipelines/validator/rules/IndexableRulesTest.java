package org.gbif.pipelines.validator.rules;

import static org.gbif.validator.api.EvaluationType.DWCA_UNREADABLE;
import static org.junit.Assert.*;

import java.util.Collections;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation.Status;
import org.junit.Test;

public class IndexableRulesTest {

  @Test
  public void indexableTest() {

    // When
    Metrics metrics =
        Metrics.builder()
            .stepTypes(
                Collections.singletonList(
                    ValidationStep.builder()
                        .stepType(StepType.ABCD_TO_VERBATIM.name())
                        .status(Status.FINISHED)
                        .build()))
            .fileInfos(
                Collections.singletonList(
                    FileInfo.builder()
                        .issues(
                            Collections.singletonList(IssueInfo.builder().issue("TEST").build()))
                        .build()))
            .build();

    // When
    boolean result = IndexableRules.isIndexable(StepType.INTERPRETED_TO_INDEX, metrics);

    // Should
    assertTrue(result);
  }

  @Test
  public void nonIndexableTest() {
    // When
    Metrics metrics =
        Metrics.builder()
            .stepTypes(
                Collections.singletonList(
                    ValidationStep.builder()
                        .stepType(StepType.ABCD_TO_VERBATIM.name())
                        .status(Status.FINISHED)
                        .build()))
            .fileInfos(
                Collections.singletonList(
                    FileInfo.builder()
                        .issues(
                            Collections.singletonList(
                                IssueInfo.builder().issue(DWCA_UNREADABLE.name()).build()))
                        .build()))
            .build();

    // When
    boolean result = IndexableRules.isIndexable(StepType.INTERPRETED_TO_INDEX, metrics);

    // Should
    assertFalse(result);
  }
}
