package org.gbif.converters.parser.xml.identifier;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * The original and still heavily used unique identifier for occurrence records, consisting of
 * InstitutionCode, CollectionCode, and CatalogNumber. It additionally includes an optional
 * unitQualifier, a GBIF specific term used for differentiating between multiple occurrences within
 * a single ABCD 2.06 record.
 */
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
      UUID datasetKey,
      String institutionCode,
      String collectionCode,
      String catalogNumber,
      @Nullable String unitQualifier) {
    this.datasetKey = Objects.requireNonNull(datasetKey, "datasetKey can't be null");
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

  public String getInstitutionCode() {
    return institutionCode;
  }

  public String getCollectionCode() {
    return collectionCode;
  }

  public String getCatalogNumber() {
    return catalogNumber;
  }

  public String getUnitQualifier() {
    return unitQualifier;
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

  @Override
  public int hashCode() {
    return Objects.hash(datasetKey, institutionCode, collectionCode, catalogNumber, unitQualifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Triplet other = (Triplet) obj;
    return Objects.equals(this.datasetKey, other.datasetKey)
        && Objects.equals(this.institutionCode, other.institutionCode)
        && Objects.equals(this.collectionCode, other.collectionCode)
        && Objects.equals(this.catalogNumber, other.catalogNumber)
        && Objects.equals(this.unitQualifier, other.unitQualifier);
  }
}
