package org.gbif.pipelines.clustering;

import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.concatIfEligible;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.hashOrNull;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.isEligibleCode;
import static org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships.isNumeric;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;

/** Utility functions for hashing records to pregroup. */
public class HashUtilities {

  /**
   * Hashes the occurrenceID, catalogCode and otherCatalogNumber into a Set of codes. The
   * catalogNumber is constructed as it's code and also prefixed in the commonly used format of
   * IC:CC:CN and IC:CN. No code in the resulting set will be numeric as the cardinality of e.g.
   * catalogNumber=100 is too high to compute, and this is intended to be used to detect explicit
   * relationships with otherCatalogNumber.
   *
   * @return a set of hashes suitable for grouping specimen related records without other filters.
   */
  public static Set<String> hashCatalogNumbers(OccurrenceFeatures o) {
    Stream<String> cats =
        Stream.of(
            hashOrNull(o.getCatalogNumber(), false),
            hashOrNull(o.getOccurrenceID(), false),
            concatIfEligible(
                ":", o.getInstitutionCode(), o.getCollectionCode(), o.getCatalogNumber()),
            concatIfEligible(":", o.getInstitutionCode(), o.getCatalogNumber()));

    if (o.getOtherCatalogNumbers() != null) {
      cats =
          Stream.concat(cats, o.getOtherCatalogNumbers().stream().map(c -> hashOrNull(c, false)));
    }

    return cats.map(OccurrenceRelationships::normalizeID)
        .filter(c -> isEligibleCode(c) && !isNumeric(c))
        .collect(Collectors.toSet());
  }
}
