package org.gbif.pipelines.fragmenter.common;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Keygen {

  private static final Long ERROR_KEY = -1L;

  public static Long getKey(HBaseLockingKeyService keygenService, boolean useTriplet, boolean useOccurrenceId,
      RecordUnit recordUnit) {

    Set<String> uniqueStrings = new HashSet<>(2);

    // Adds occurrenceId
    if (useOccurrenceId) {
      String occurrenceId = recordUnit.getOccurrenceId();
      if (!Strings.isNullOrEmpty(occurrenceId)) {
        uniqueStrings.add(occurrenceId);
      }
    }

    // Adds triplet
    if (useTriplet) {
      String ic = recordUnit.getInstitutionCode();
      String cc = recordUnit.getCollectionCode();
      String cn = recordUnit.getCatalogNumber();
      OccurrenceKeyBuilder.buildKey(ic, cc, cn).ifPresent(uniqueStrings::add);
    }

    if (uniqueStrings.isEmpty()) {
      return ERROR_KEY;
    }

    // Finds or generate key
    KeyLookupResult keyResult = Optional.ofNullable(keygenService.findKey(uniqueStrings))
        .orElse(keygenService.generateKey(uniqueStrings));
    return Optional.ofNullable(keyResult).map(KeyLookupResult::getKey).orElse(ERROR_KEY);
  }

  public static Long getErrorKey() {
    return ERROR_KEY;
  }
}
