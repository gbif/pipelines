package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

@Slf4j
public class MaxStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {

    Optional<Long> tripletKey = Optional.empty();
    if (triplet != null && !triplet.isEmpty()) {
      tripletKey =
          Optional.ofNullable(keygenService.findKey(Collections.singleton(triplet)))
              .map(KeyLookupResult::getKey);
    }

    Optional<Long> occurrenceIdtKey = Optional.empty();
    if (occurrenceId != null && !occurrenceId.isEmpty()) {
      occurrenceIdtKey =
          Optional.ofNullable(keygenService.findKey(Collections.singleton(occurrenceId)))
              .map(KeyLookupResult::getKey);
    }

    if (!tripletKey.isPresent() || !occurrenceIdtKey.isPresent()) {
      return Collections.emptySet();
    }

    if (tripletKey.get().equals(occurrenceIdtKey.get())) {
      return Collections.emptySet();
    }

    long max = Math.max(tripletKey.get(), occurrenceIdtKey.get());
    return max == tripletKey.get()
        ? Collections.singleton(triplet)
        : Collections.singleton(occurrenceId);
  }
}
