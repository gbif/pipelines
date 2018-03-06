package org.gbif.pipelines.interpretation.parsers.taxonomy;

import org.gbif.api.v2.NameUsageMatch2;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Utility class for the taxonomic interpretation.
 */
public final class TaxonomyValidator {

  private TaxonomyValidator() {}

  /**
   * Creates a predicate to test if a {@link NameUsageMatch2} is empty.
   *
   * @return {@link Predicate}
   */
  public static boolean isEmpty(NameUsageMatch2 response) {
    return Objects.isNull(response)
           || Objects.isNull(response.getUsage())
           || Objects.isNull(response.getClassification())
           || Objects.isNull(response.getDiagnostics());
  }

}
