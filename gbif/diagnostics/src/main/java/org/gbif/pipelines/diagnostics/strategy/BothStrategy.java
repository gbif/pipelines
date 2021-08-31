package org.gbif.pipelines.diagnostics.strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class BothStrategy implements DeletionStrategy {
  @Override
  public Map<String, Long> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    Map<String, Long> keys = new HashMap<>(2);
    if (!onlyCollisions) {
      tripletKey.ifPresent(x -> keys.put(triplet, x));
      occurrenceIdtKey.ifPresent(x -> keys.put(occurrenceId, x));
    }

    if (onlyCollisions
        && tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.put(triplet, tripletKey.get());
      keys.put(occurrenceId, occurrenceIdtKey.get());
    }

    return keys;
  }
}
