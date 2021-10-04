package org.gbif.validator.api;

import static org.gbif.validator.api.EvaluationType.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Class defining the "rule" to determine if a resource can be indexed or not. TODO when (if?)
 * needed, add the mapping between the ValidationProfile and the rule
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

  /** Return {@link EvaluationType} that make a resource non-indexable. */
  public static Set<EvaluationType> getNonIndexableEvaluationType() {
    return NON_INDEXABLE_EVALUATION_TYPE;
  }

  /** Given a list of {@link EvaluationType}, determine if the resource is indexable or not. */
  public static boolean isIndexable(List<EvaluationType> resultElements) {
    return resultElements.stream().noneMatch(NON_INDEXABLE_EVALUATION_TYPE::contains);
  }

  /** Given a list of {@link EvaluationType}, determine if the resource is indexable or not. */
  public static boolean isIndexable(EvaluationType resultElements) {
    return !NON_INDEXABLE_EVALUATION_TYPE.contains(resultElements);
  }
}
