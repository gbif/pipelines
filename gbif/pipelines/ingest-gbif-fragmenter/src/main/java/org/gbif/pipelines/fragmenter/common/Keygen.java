package org.gbif.pipelines.fragmenter.common;

import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Keygen {

  private static final Long ERROR_KEY = -1L;

  /** Get or generate GBIF ID key */
  public static Long getKey(
      HBaseLockingKeyService keygenService,
      boolean useTriplet,
      boolean useOccurrenceId,
      OccurrenceRecord record) {

    Set<String> uniqueStrings = new HashSet<>(2);

    // Adds occurrenceId
    if (useOccurrenceId) {
      String occurrenceId = record.getOccurrenceId();
      if (!Strings.isNullOrEmpty(occurrenceId)) {
        uniqueStrings.add(occurrenceId);
      }
    }

    // Adds triplet
    if (useTriplet) {
      String ic = record.getInstitutionCode();
      String cc = record.getCollectionCode();
      String cn = record.getCatalogNumber();
      OccurrenceKeyBuilder.buildKey(ic, cc, cn).ifPresent(uniqueStrings::add);
    }

    if (uniqueStrings.isEmpty()) {
      return ERROR_KEY;
    }

    KeyLookupResult keyResult = null;
    try {
      // Finds or generates key
      keyResult = keygenService.findKey(uniqueStrings);
      if (keyResult == null) {
        log.error("GBIF ID wasn't found, generating a new key.");
        keyResult = keygenService.generateKey(uniqueStrings);
      }
    } catch (RuntimeException ex) {
      log.error(ex.getMessage(), ex);
    }

    return Optional.ofNullable(keyResult).map(KeyLookupResult::getKey).orElse(ERROR_KEY);
  }

  public static String getSaltedKey(Long key) {
    long salt = key % 100;
    String result = salt + ":" + key;
    return salt >= 10 ? result : "0" + result;
  }

  public static Long getErrorKey() {
    return ERROR_KEY;
  }
}
