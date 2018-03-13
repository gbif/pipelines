package org.gbif.xml.occurrence.parser.identifier;

import javax.annotation.Nullable;

/**
 * A helper class for building the row keys used in the occurrence lookup table/process.
 * TODO: this is too similar to OccurrenceKeyBuilder - they should be merged
 */
public class OccurrenceKeyHelper {

  private static final char DELIM = '|';

  private OccurrenceKeyHelper() {
  }

  public static String buildKeyPrefix(String datasetKey) {
    return datasetKey + DELIM;
  }

  @Nullable
  public static String buildKey(@Nullable HolyTriplet triplet) {
    if (triplet == null
        || triplet.getDatasetKey() == null
        || triplet.getInstitutionCode() == null
        || triplet.getCollectionCode() == null
        || triplet.getCatalogNumber() == null) {
      return null;
    }

    return triplet.getDatasetKey().toString()
           + DELIM
           + triplet.getInstitutionCode()
           + DELIM
           + triplet.getCollectionCode()
           + DELIM
           + triplet.getCatalogNumber()
           + DELIM
           + triplet.getUnitQualifier();
  }

  @Nullable
  public static String buildKey(@Nullable PublisherProvidedUniqueIdentifier pubProvided) {
    if (pubProvided == null
        || pubProvided.getDatasetKey() == null
        || pubProvided.getPublisherProvidedIdentifier() == null) {
      return null;
    }

    return pubProvided.getDatasetKey().toString() + DELIM + pubProvided.getPublisherProvidedIdentifier();
  }

  @Nullable
  public static String buildUnscopedKey(@Nullable PublisherProvidedUniqueIdentifier pubProvidedUniqueId) {
    if (pubProvidedUniqueId == null) {
      return null;
    }

    return pubProvidedUniqueId.getPublisherProvidedIdentifier();
  }

  @Nullable
  public static String buildUnscopedKey(@Nullable HolyTriplet triplet) {
    if (triplet == null
        || triplet.getDatasetKey() == null
        || triplet.getInstitutionCode() == null
        || triplet.getCollectionCode() == null
        || triplet.getCatalogNumber() == null) {
      return null;
    }

    return triplet.getInstitutionCode()
           + DELIM
           + triplet.getCollectionCode()
           + DELIM
           + triplet.getCatalogNumber()
           + DELIM
           + triplet.getUnitQualifier();
  }
}
