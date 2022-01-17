package org.gbif.pipelines.validator.rules;

import static org.gbif.validator.api.EvaluationType.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation.Status;

/**
 * Class defining the "rule" to determine if a resource can be indexed or not. needed, add the
 * mapping between the ValidationProfile and the rule
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IndexableRules {

  /** {@link EvaluationType} that makes the resource non-indexable. */
  private static final Set<EvaluationType> NON_INDEXABLE_EVALUATION_TYPE =
      new HashSet<>(
          Arrays.asList(
              DWCA_UNREADABLE,
              DWCA_META_XML_NOT_FOUND,
              DWCA_META_XML_SCHEMA,
              UNREADABLE_SECTION_ERROR,
              CORE_ROWTYPE_UNDETERMINED,
              RECORD_IDENTIFIER_NOT_FOUND,
              RECORD_REFERENTIAL_INTEGRITY_VIOLATION,
              RECORD_NOT_UNIQUELY_IDENTIFIED,
              OCCURRENCE_NOT_UNIQUELY_IDENTIFIED,
              DUPLICATED_TERM,
              LICENSE_MISSING_OR_UNKNOWN));

  /** Check that all steps were completed(except current) and there is no non-indexable issue */
  public static boolean isIndexable(StepType ignoreStepType, Metrics metrics) {
    boolean finishedAllSteps =
        metrics.getStepTypes().stream()
            .filter(x -> !x.getStepType().equals(ignoreStepType.name()))
            .noneMatch(y -> y.getStatus() != Status.FINISHED);

    boolean noNonIndexableIssue =
        metrics.getFileInfos().stream()
            .map(FileInfo::getIssues)
            .flatMap(Collection::stream)
            .map(
                x -> {
                  try {
                    return EvaluationType.valueOf(x.getIssue());
                  } catch (Exception ex) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .noneMatch(NON_INDEXABLE_EVALUATION_TYPE::contains);

    return finishedAllSteps && noNonIndexableIssue;
  }
}
