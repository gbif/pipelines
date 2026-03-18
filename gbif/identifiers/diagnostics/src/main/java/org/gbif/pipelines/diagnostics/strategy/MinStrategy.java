package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class MinStrategy implements DeletionStrategy {
  @Override
  public Map<String, Long> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    if (!tripletKey.isPresent() || !occurrenceIdtKey.isPresent()) {
      return Collections.emptyMap();
    }

    if (tripletKey.get().equals(occurrenceIdtKey.get())) {
      return Collections.emptyMap();
    }

    long min = Math.min(tripletKey.get(), occurrenceIdtKey.get());
    return min == tripletKey.get()
        ? Collections.singletonMap(triplet, tripletKey.get())
        : Collections.singletonMap(occurrenceId, occurrenceIdtKey.get());
  }
}
