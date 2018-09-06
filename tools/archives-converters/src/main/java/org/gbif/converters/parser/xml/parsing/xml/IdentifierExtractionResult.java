package org.gbif.converters.parser.xml.parsing.xml;

import org.gbif.converters.parser.xml.identifier.UniqueIdentifier;

import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contains the set of {@link UniqueIdentifier} that were extracted from a raw fragment that each
 * uniquely identify that fragment. May also contain a unitQualifier for cases of multiple
 * occurrences within a single fragment (e.g. ABCD 2.06).
 */
public class IdentifierExtractionResult {

  private final Set<UniqueIdentifier> uniqueIdentifiers;
  @Nullable private final String unitQualifier;

  public IdentifierExtractionResult(
      Set<UniqueIdentifier> uniqueIdentifiers, @Nullable String unitQualifier) {
    this.uniqueIdentifiers = checkNotNull(uniqueIdentifiers, "uniqueIdentifiers can't be null");
    this.unitQualifier = unitQualifier;
  }

  public String getUnitQualifier() {
    return unitQualifier;
  }

  public Set<UniqueIdentifier> getUniqueIdentifiers() {
    return uniqueIdentifiers;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uniqueIdentifiers, unitQualifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final IdentifierExtractionResult other = (IdentifierExtractionResult) obj;
    return Objects.equal(this.uniqueIdentifiers, other.uniqueIdentifiers)
        && Objects.equal(this.unitQualifier, other.unitQualifier);
  }
}
