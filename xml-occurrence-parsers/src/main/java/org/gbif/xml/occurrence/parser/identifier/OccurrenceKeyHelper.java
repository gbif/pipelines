package org.gbif.xml.occurrence.parser.identifier;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A helper class for building the row keys used in the occurrence lookup table/process. TODO: this
 * is too similar to OccurrenceKeyBuilder - they should be merged
 */
public class OccurrenceKeyHelper {

  private static final String DELIM = "|";

  private OccurrenceKeyHelper() {}

  public static String buildKeyPrefix(String datasetKey) {
    return datasetKey + DELIM;
  }

  @Nullable
  public static String buildKey(@Nullable Triplet triplet) {
    if (Objects.isNull(triplet)
        || Objects.isNull(triplet.getDatasetKey())
        || Objects.isNull(triplet.getInstitutionCode())
        || Objects.isNull(triplet.getCollectionCode())
        || Objects.isNull(triplet.getCatalogNumber())) {
      return null;
    }

    return join(
        DELIM,
        triplet.getDatasetKey().toString(),
        triplet.getInstitutionCode(),
        triplet.getCollectionCode(),
        triplet.getCatalogNumber(),
        triplet.getUnitQualifier());
  }

  @Nullable
  public static String buildKey(@Nullable PublisherProvidedUniqueIdentifier pubProvided) {
    if (Objects.isNull(pubProvided)
        || Objects.isNull(pubProvided.getDatasetKey())
        || Objects.isNull(pubProvided.getPublisherProvidedIdentifier())) {
      return null;
    }

    return join(
        DELIM,
        pubProvided.getDatasetKey().toString(),
        pubProvided.getPublisherProvidedIdentifier());
  }

  @Nullable
  public static String buildUnscopedKey(
      @Nullable PublisherProvidedUniqueIdentifier pubProvidedUniqueId) {
    if (pubProvidedUniqueId == null) {
      return null;
    }

    return pubProvidedUniqueId.getPublisherProvidedIdentifier();
  }

  @Nullable
  public static String buildUnscopedKey(@Nullable Triplet triplet) {
    if (Objects.isNull(triplet)
        || Objects.isNull(triplet.getDatasetKey())
        || Objects.isNull(triplet.getInstitutionCode())
        || Objects.isNull(triplet.getCollectionCode())
        || Objects.isNull(triplet.getCatalogNumber())) {
      return null;
    }

    return join(
        DELIM,
        triplet.getInstitutionCode(),
        triplet.getCollectionCode(),
        triplet.getCatalogNumber(),
        triplet.getUnitQualifier());
  }

  @Nullable
  public static String toKey(@Nullable Triplet triplet) {
    if (Objects.isNull(triplet)
        || Objects.isNull(triplet.getInstitutionCode())
        || Objects.isNull(triplet.getCollectionCode())
        || Objects.isNull(triplet.getCatalogNumber())) {
      return null;
    }

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    return join(
        ":",
        "urn:catalog",
        triplet.getInstitutionCode(),
        triplet.getCollectionCode(),
        triplet.getCatalogNumber());
  }

  private static String join(String delim, String... values) {
    StringJoiner joiner = new StringJoiner(delim);
    Arrays.stream(values)
        .forEach(x -> Optional.ofNullable(x).filter(f -> !f.isEmpty()).ifPresent(joiner::add));
    return joiner.toString();
  }
}
