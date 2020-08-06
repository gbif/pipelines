package org.gbif.converters.parser.xml.parsing.xml;

import java.util.Set;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.gbif.converters.parser.xml.identifier.UniqueIdentifier;

/**
 * Contains the set of {@link UniqueIdentifier} that were extracted from a raw fragment that each
 * uniquely identify that fragment. May also contain a unitQualifier for cases of multiple
 * occurrences within a single fragment (e.g. ABCD 2.06).
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class IdentifierExtractionResult {

  @NonNull private final Set<UniqueIdentifier> uniqueIdentifiers;

  @Nullable private final String unitQualifier;
}
