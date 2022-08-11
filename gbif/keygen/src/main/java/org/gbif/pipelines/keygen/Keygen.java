package org.gbif.pipelines.keygen;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Keygen {

  private static final Long ERROR_KEY = -1L;

  /** Get or generate GBIF ID key */
  public static Optional<Long> getKey(
      HBaseLockingKey keygenService,
      boolean useTriplet,
      boolean useOccurrenceId,
      boolean generateIfAbsent,
      OccurrenceRecord record) {

    Set<String> uniqueStrings = new HashSet<>(2);

    // Adds occurrenceId
    if (useOccurrenceId) {
      Optional<String> occurrenceId = record.getOccurrenceId();
      if (occurrenceId.isPresent()) {
        Optional<KeyLookupResult> keyForOccurrence =
            keygenService.findKey(Collections.singleton(occurrenceId.get()));
        if (keyForOccurrence.isPresent()) {
          return Optional.of(keyForOccurrence.get().getKey());
        } else {
          uniqueStrings.add(occurrenceId.get());
        }
      }
    }

    // Adds triplet, if useTriplet and useOccurrenceId is false, or occurrenceId is null
    if (useTriplet) {
      if (uniqueStrings.isEmpty()) {
        record.getTriplet().ifPresent(uniqueStrings::add);
      } else {
        record
            .getTriplet()
            .filter(t -> keygenService.findKey(Collections.singleton(t)).isPresent())
            .ifPresent(uniqueStrings::add);
      }
    }

    if (uniqueStrings.isEmpty()) {
      return Optional.of(ERROR_KEY);
    }

    Optional<KeyLookupResult> keyResult;
    try {
      // Finds or generates key
      keyResult = keygenService.findKey(uniqueStrings);
      if (!keyResult.isPresent() && generateIfAbsent) {
        log.error("GBIF ID wasn't found, generating a new key.");
        keyResult = Optional.of(keygenService.generateKey(uniqueStrings));
      }
    } catch (RuntimeException ex) {
      log.error(ex.getMessage(), ex);
      return Optional.of(ERROR_KEY);
    }

    return keyResult.map(KeyLookupResult::getKey);
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
