package org.gbif.pipelines.diagnostics.strategy;

import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class MaxStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

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
