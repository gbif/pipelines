package org.gbif.converters.parser.xml.identifier;

import com.google.common.base.Preconditions;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * The original and still heavily used unique identifier for occurrence records, consisting of
 * InstitutionCode, CollectionCode, and CatalogNumber. It additionally includes an optional
 * unitQualifier, a GBIF specific term used for differentiating between multiple occurrences within
 * a single ABCD 2.06 record.
 */
@EqualsAndHashCode
@Getter
public class Triplet implements UniqueIdentifier {

  private final UUID datasetKey;
  private final String institutionCode;
  private final String collectionCode;
  private final String catalogNumber;
  private final String unitQualifier;

  /**
   * Constructs the immutable triplet.
   *
   * @throws IllegalArgumentException if institutionCode, collectionCode, or catalogNumber are null
   *     or empty
   * @throws NullPointerException if datasetKey is null
   */
  public Triplet(
      @NonNull UUID datasetKey,
      String institutionCode,
      String collectionCode,
      String catalogNumber,
      @Nullable String unitQualifier) {
    this.datasetKey = datasetKey;
    Preconditions.checkArgument(
        institutionCode != null && !institutionCode.isEmpty(),
        "institutionCode can't be null or empty");
    Preconditions.checkArgument(
        collectionCode != null && !collectionCode.isEmpty(),
        "collectionCode can't be null or empty");
    Preconditions.checkArgument(
        catalogNumber != null && !catalogNumber.isEmpty(), "catalogNumber can't be null or empty");
    this.institutionCode = institutionCode;
    this.collectionCode = collectionCode;
    this.catalogNumber = catalogNumber;
    this.unitQualifier = unitQualifier;
  }

  public Triplet(String institutionCode, String collectionCode, String catalogNumber) {
    Preconditions.checkArgument(
        institutionCode != null && !institutionCode.isEmpty(),
        "institutionCode can't be null or empty");
    Preconditions.checkArgument(
        collectionCode != null && !collectionCode.isEmpty(),
        "collectionCode can't be null or empty");
    Preconditions.checkArgument(
        catalogNumber != null && !catalogNumber.isEmpty(), "catalogNumber can't be null or empty");
    this.institutionCode = institutionCode;
    this.collectionCode = collectionCode;
    this.catalogNumber = catalogNumber;
    datasetKey = null;
    unitQualifier = null;
  }

  @Override
  public UUID getDatasetKey() {
    return datasetKey;
  }

  @Override
  public String getUniqueString() {
    return OccurrenceKeyHelper.buildKey(this);
  }

  @Override
  public String getUnscopedUniqueString() {
    return OccurrenceKeyHelper.buildUnscopedKey(this);
  }
}
