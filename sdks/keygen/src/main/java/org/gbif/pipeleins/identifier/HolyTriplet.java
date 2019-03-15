package org.gbif.pipeleins.identifier;

import java.util.UUID;

import com.google.common.base.Objects;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The original and still heavily used unique identifier for occurrence records, consisting of InstitutionCode,
 * CollectionCode, and CatalogNumber. It additionally includes an optional unitQualifier, a GBIF specific term used
 * for differentiating between multiple occurrences within a single ABCD 2.06 record.
 */
public class HolyTriplet implements UniqueIdentifier {

  private final UUID datasetKey;
  private final String institutionCode;
  private final String collectionCode;
  private final String catalogNumber;
  private final String unitQualifier;

  /**
   * Constructs the immutable triplet.
   *
   * @throws IllegalArgumentException if institutionCode, collectionCode, or catalogNumber are null or empty
   * @throws NullPointerException if datasetKey is null
   */
  public HolyTriplet(UUID datasetKey, String institutionCode, String collectionCode, String catalogNumber,
    @Nullable String unitQualifier) {
    this.datasetKey = checkNotNull(datasetKey, "datasetKey can't be null");
    checkArgument(institutionCode != null && !institutionCode.isEmpty(), "institutionCode can't be null or empty");
    checkArgument(collectionCode != null && !collectionCode.isEmpty(), "collectionCode can't be null or empty");
    checkArgument(catalogNumber != null && !catalogNumber.isEmpty(), "catalogNumber can't be null or empty");
    this.institutionCode = institutionCode;
    this.collectionCode = collectionCode;
    this.catalogNumber = catalogNumber;
    this.unitQualifier = unitQualifier;
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
    return Objects.hashCode(datasetKey, institutionCode, collectionCode, catalogNumber, unitQualifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final HolyTriplet other = (HolyTriplet) obj;
    return Objects.equal(this.datasetKey, other.datasetKey) && Objects
      .equal(this.institutionCode, other.institutionCode) && Objects.equal(this.collectionCode, other.collectionCode)
           && Objects.equal(this.catalogNumber, other.catalogNumber) && Objects
      .equal(this.unitQualifier, other.unitQualifier);
  }
}
