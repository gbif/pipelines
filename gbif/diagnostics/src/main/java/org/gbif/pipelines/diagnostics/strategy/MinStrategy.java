package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class MinStrategy implements DeletionStrategy {
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

    long min = Math.min(tripletKey.get(), occurrenceIdtKey.get());
    return min == tripletKey.get()
        ? Collections.singleton(triplet)
        : Collections.singleton(occurrenceId);
  }
}
